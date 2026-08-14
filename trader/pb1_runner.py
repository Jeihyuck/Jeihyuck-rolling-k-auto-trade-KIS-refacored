from __future__ import annotations

import argparse
import json
import logging
import os
import re
import signal
import traceback
import time as time_mod
import copy
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

from trader import install_legacy_pb1_runtime_guards

# ``pb1_runner`` is the legacy Korean-market runner.  Keep its compatibility
# guards here instead of eagerly importing them for every ``trader.us`` module.
install_legacy_pb1_runtime_guards()

from trader.contracts.final30_contract import assert_final30_contract
from trader.final30_quality import (
    format_final30_abort_message,
    normalize_final30_contract_row,
    summarize_entry_style_distribution,
    validate_trade_ready,
    verify_final30_scored_rows,
)
from zoneinfo import ZoneInfo

import pandas as pd

from trader.constants import FLOW_OPTIONAL_COLS, REQUIRED_FINAL30_SCORED_COLS
from trader.account_state import (
    env_flag as account_env_flag,
    expected_initial_holdings,
    expected_practice_capital_krw,
    get_account_key,
    get_masked_account_key,
    resolve_account_sanity_capital_tolerance_krw,
)
from trader.runtime_paths import build_final30_scored_paths, repo_root
from trader.path_contract import build_final30_paths, build_watchlist_paths, read_final30_file_rows, resolve_repo_root, serialize_path_map, write_final30_mirrors

from trader.config import (
    AFTERNOON_WINDOW_END,
    AFTERNOON_WINDOW_START,
    CANDIDATE_POOL_TTL_DAYS,
    CLOSE_AUCTION_END,
    CLOSE_AUCTION_START,
    DERIVED_FALLBACK_ENABLED,
    DERIVED_FALLBACK_MAX_DAYS,
    DERIVED_FALLBACK_WARN_AGE_DAYS,
    DIAGNOSTIC_MODE,
    DIAGNOSTIC_ONLY,
    LEDGER_BASE_DIR,
    LEDGER_LOOKBACK_DAYS,
    MARKET_CLOSE_HHMM,
    MARKET_OPEN_HHMM,
    MORNING_EXIT_END,
    MORNING_EXIT_START,
    MORNING_WINDOW_END,
    MORNING_WINDOW_START,
    NONTRADING_SMOKE_DB_STORE,
    NONTRADING_SMOKE_FORCE,
    NONTRADING_SMOKE_FORCE_REBUILD,
    NONTRADING_SMOKE_TIMEOUT_SEC,
    PB1_ALLOW_PREOPEN_ENTRY,
    PB1_PREOPEN_END,
    PB1_PREOPEN_MAX_NEW_POSITIONS,
    PB1_PREOPEN_REQUIRE_BALANCE,
    PB1_PREOPEN_START,
    PB1_ENTRY_ENABLED,
    PB1_ENTRY_WINDOW_END,
    PB1_EXIT_WINDOW_END,
    PB1_MORNING_WINDOW_END,
    PB1_REQUIRE_BALANCE_FOR_ENTRY,
    PB1_DIAG_IGNORE_ENTRY_CUTOFF,
    PB1_FORCE_ENTRY_ON_PUSH,
    PB1_MAX_WAIT_FOR_WINDOW_MIN,
    PB1_WAIT_FOR_WINDOW,
    PAPER_MAX_CAPITAL_KRW,
    PAPER_RESET_AUTO_PURGE,
    PAPER_RESET_EVENT_ONLY_IN_PRACTICE,
    resolve_strategy_mode,
)
from trader.runtime_paths import runtime_root, runtime_path
from trader.logging_utils import append_jsonl
from trader.db.engine import make_engine, dispose_engine_safely
from trader.db.health import assert_db_ready
from trader.db.locks import acquire_advisory_xact_lock, release_advisory_xact_lock
from trader.db.migrate import run_migrations
from trader.db.repos import (
    FINAL30_SCORED_DB_CONTRACT_FIELDS,
    FillsRepo,
    DerivedMinerviniRepo,
    LedgerEventsRepo,
    OrdersRepo,
    PositionsRepo,
    PracticeAccountResetRepo,
    ReconcileLogRepo,
    RunsRepo,
    ScoredWatchlistInvalidError,
    ScoredWatchlistNotFoundError,
    UniverseRepo,
    load_final30_scored_exact,
    load_final30_scored_db_only,
    load_job_checkpoint,
    save_job_checkpoint,
)
from trader.diagnostics.nontrading_smoke import (
    nontrading_smoke_flag_path,
    run_nontrading_smoke_once,
    write_nontrading_smoke_flag,
)
from trader.kis_wrapper import KisAPI, KisBalanceUnavailable, KisTemporaryError, get_breaker_runtime_stats, get_price_runtime_stats
from trader.pb1_engine import PB1Engine, UniverseContext, resolve_pb1_phase
from trader.entry_engine import scan_all_strategies, calculate_position_size
from trader.reconcile_kis import reconcile_kis, reconcile_today
from trader.reconcile_db import close_stale_positions
from trader.run_context import RunContext
from trader.universe.build import build_universe
from trader.universe.mode import is_db_only_mode
from trader.time_utils import calc_market_window_kst, is_trading_weekday, now_kst, week_monday, is_market_open_kst, market_close_dt_kst, resolve_derived_as_of, resolve_trade_context, prev_business_day
from trader.kr.calendar import resolve_kr_expected_as_of, resolve_kr_trade_date
from trader.kr.market_scope import is_kr_market
from trader.utils.env import env_bool, parse_env_flag, resolve_mode, parse_bool_any
from trader.window_router import WindowDecision, decide_window
from trader.watchlist_builder import build_and_save_watchlist
from trader.db.repos import WatchlistRepo
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.indicators import compute_ma20_from_ohlcv, safe_nullable_float
from trader.strategies.pb1_minervini_v2 import MinerviniConfig

logger = logging.getLogger(__name__)
log = logger

SESSION_WARNING_KEYS = (
    "timeout_count",
    "db_read_fail_open_count",
    "ledger_fail_first_count",
    "degraded_stage_count",
    "duplicate_skip_count",
)


def try_acquire_lock(*args, **kwargs):
    return acquire_advisory_xact_lock(*args, **kwargs)


def is_trading_day(now: datetime) -> bool:
    return is_trading_weekday(now)

OPTIONAL_SCORING_ALTERNATIVE_COLS = [("close", "last_close")]

_FINAL30_LOAD_CACHE: dict[tuple[str, str], dict[str, Any]] = {}


def _missing_scored_cols(columns: list[str]) -> list[str]:
    cols = {str(c) for c in (columns or [])}
    missing = [c for c in REQUIRED_FINAL30_SCORED_COLS if c not in cols]
    for primary, alternative in OPTIONAL_SCORING_ALTERNATIVE_COLS:
        if primary in missing and alternative in cols:
            missing.remove(primary)
    return missing


def _safe_flow_optional_missing(columns: list[str]) -> list[str]:
    try:
        flow_optional_cols = globals().get("FLOW_OPTIONAL_COLS", [])
        return [col for col in flow_optional_cols if col not in set(columns or [])]
    except Exception as exc:
        logger.warning(
            "[FINAL30][FLOW_CHECK_GUARD] optional flow check failed err=%s",
            exc,
        )
        return []


def _manual_test_route_reasons(*, mode: str) -> list[str]:
    if (mode or "").strip().upper() != "DIAG":
        return []
    reasons: list[str] = []
    if env_bool("PB1_DIAG_FULL_EXEC", default=False):
        reasons.append("diag_full_exec")
    if env_bool("FORCE_RUN", default=False):
        reasons.append("force_run")
    if env_bool("WATCHLIST_MODE", default=False):
        reasons.append("watchlist_mode")
    return reasons


def _normalize_warning_counts(raw: dict[str, Any] | None) -> dict[str, int]:
    counts = {key: 0 for key in SESSION_WARNING_KEYS}
    for key, value in dict(raw or {}).items():
        try:
            counts[str(key)] = int(value or 0)
        except Exception:
            counts[str(key)] = 0
    return counts


def _merge_warning_counts(base: dict[str, int], incoming: dict[str, Any] | None) -> dict[str, int]:
    merged = dict(base)
    for key, value in _normalize_warning_counts(incoming).items():
        merged[key] = int(merged.get(key, 0)) + int(value)
    return merged


def _warning_total(counts: dict[str, Any] | None) -> int:
    return sum(int(value or 0) for value in dict(counts or {}).values())


def _resolve_session_terminal_state(*, result_status: str, exit_reason: str, warning_counts: dict[str, Any] | None) -> str:
    status = str(result_status or "UNKNOWN").strip().upper()
    exit_reason_norm = str(exit_reason or "").strip().lower()
    counts = _normalize_warning_counts(warning_counts)
    if status in {"FATAL_RUNTIME", "FATAL_POSTPROCESS"}:
        return "SESSION_END_FATAL"
    if status.startswith("SKIP") or exit_reason_norm.startswith("phase_guard_skip"):
        return "SESSION_END_SKIPPED"
    if status in {"OK_DEGRADED", "DEGRADED_POSTPROCESS"} or counts.get("degraded_stage_count", 0) > 0:
        return "SESSION_END_OK_DEGRADED"
    if _warning_total(counts) > 0 or status in {"WARN_FAIL_OPEN", "OK_WITH_WARNINGS"}:
        return "SESSION_END_OK_WITH_WARNINGS"
    return "SESSION_END_OK"


def _runtime_fatal_signature(exc: Exception) -> tuple[str, str]:
    return type(exc).__name__, str(exc).strip() or "<empty>"


def _update_runtime_fatal_guard(
    *,
    previous_signature: tuple[str, str] | None,
    previous_count: int,
    exc: Exception,
    repeat_threshold: int,
) -> tuple[tuple[str, str], int, bool]:
    signature = _runtime_fatal_signature(exc)
    count = previous_count + 1 if previous_signature == signature else 1
    return signature, count, count >= max(2, int(repeat_threshold or 2))


def evaluate_workflow_log_success(*, session: str, log_text: str) -> dict[str, Any]:
    text = str(log_text or "")
    result_matches = re.findall(r"\[RUN_SUMMARY\]\[RESULT\] status=([A-Z_]+) reason=([^\s]+)", text)
    result_status, result_reason = result_matches[-1] if result_matches else ("", "")
    terminal_matches = re.findall(r"\[PB1\]\[SESSION_TERMINAL\] terminal_state=([A-Z_]+) result_status=([A-Z_]+) exit_reason=([^\s]+)", text)
    terminal_state = terminal_matches[-1][0] if terminal_matches else ""
    session_end_release = bool(re.search(r"\[PB1\]\[LOOP\]\[SESSION_END_RELEASE\]|\[PB1\]\[EXIT\] reason=session_end", text))
    tick_seen = bool(re.search(r"\[PB1\]\[TICK\]\[DONE\]|\[PB1\]\[TICK_TIMEOUT\]|\[PB1\]\[SESSION\]\[WARNINGS\]", text))
    traceback_seen = bool(re.search(r"Traceback \(most recent call last\):", text))
    fatal_runtime = bool(re.search(r"\[RUN_SUMMARY\]\[RESULT\] status=(FATAL_RUNTIME|FATAL_POSTPROCESS)|\[PB1\]\[EXIT\] reason=fatal_runtime", text))
    timeout_count = len(re.findall(r"TickTimeoutError|tick_hard_timeout|\[WARN\]\[PB1\]\[STAGE_TIMEOUT\]", text))
    db_autocommit_count = len(re.findall(r"can't change 'autocommit' now|connection in transaction status ACTIVE", text))
    degraded_count = len(re.findall(r"DEGRADED_POSTPROCESS|\[PB1\]\[POSTPROCESS\]\[DEGRADED\]|\[DEGRADED\]\[PB1\]", text))
    exit_pass_timeout_observed = int(bool(re.search(r"\[PB1\]\[POST_CAPITAL\]\[EXIT_PASS\]\[(TIMEOUT|DEGRADED)\]", text)))
    warnings_present = any(value > 0 for value in (timeout_count, db_autocommit_count, degraded_count, exit_pass_timeout_observed))

    if not result_status:
        return {
            "ok": False,
            "status": "VERIFY_FAIL",
            "reason": "missing_run_summary",
            "timeout_count": timeout_count,
            "db_autocommit_count": db_autocommit_count,
            "degraded_count": degraded_count,
            "exit_pass_timeout_observed": exit_pass_timeout_observed,
        }

    skip_reason = str(result_reason or "").lower()
    policy_skip_failure = result_status.startswith("SKIP") and any(
        token in skip_reason for token in ("duplicate", "stale", "invalid_window")
    )
    clean_success = result_status in {"OK", "OK_NO_TRADE", "OK_DEGRADED", "OK_WITH_WARNINGS", "OK_MANUAL_REPLAY"}
    if terminal_state and terminal_state.startswith("SESSION_END_OK"):
        clean_success = True
    if fatal_runtime or traceback_seen:
        return {
            "ok": False,
            "status": result_status or "VERIFY_FAIL",
            "reason": "fatal_detected",
            "timeout_count": timeout_count,
            "db_autocommit_count": db_autocommit_count,
            "degraded_count": degraded_count,
            "exit_pass_timeout_observed": exit_pass_timeout_observed,
        }
    if policy_skip_failure:
        return {
            "ok": False,
            "status": result_status,
            "reason": result_reason or "policy_skip_failure",
            "timeout_count": timeout_count,
            "db_autocommit_count": db_autocommit_count,
            "degraded_count": degraded_count,
            "exit_pass_timeout_observed": exit_pass_timeout_observed,
        }
    if not clean_success and not (session_end_release and tick_seen):
        return {
            "ok": False,
            "status": result_status,
            "reason": "session_not_alive",
            "timeout_count": timeout_count,
            "db_autocommit_count": db_autocommit_count,
            "degraded_count": degraded_count,
            "exit_pass_timeout_observed": exit_pass_timeout_observed,
        }
    if result_status == "OK_DEGRADED" or degraded_count > 0:
        verify_status = "OK_DEGRADED"
    elif warnings_present:
        verify_status = "OK_WITH_WARNINGS"
    else:
        verify_status = result_status
    return {
        "ok": True,
        "status": verify_status,
        "reason": result_reason or "session_completed",
        "timeout_count": timeout_count,
        "db_autocommit_count": db_autocommit_count,
        "degraded_count": degraded_count,
        "exit_pass_timeout_observed": exit_pass_timeout_observed,
    }


def _load_json_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return [dict(x) for x in items if isinstance(x, dict)]
        return []
    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]
    return []


def _validate_trade_final30_files(*, repo_root_path: Path, env: str, as_of: str) -> dict[str, Any]:
    path_results: dict[str, Any] = {}
    aggregate_ok = True
    aggregate_errors: list[str] = []
    for label, path in build_final30_paths(repo_root_path, env, as_of).items():
        rows, json_ok = read_final30_file_rows(path)
        summary = summarize_entry_style_distribution(rows)
        logger.info(
            "[TRADE][FINAL30][ENTRY_STYLE_DISTRIBUTION] total=%s counts=%s",
            int(summary.get("total") or 0),
            dict(summary.get("counts") or {}),
        )
        result = verify_final30_scored_rows(
            rows,
            required_rows=30,
            required_fields=REQUIRED_FINAL30_SCORED_COLS,
            source=f"FILE:{label}",
        ) if json_ok and rows else {
            "ok": False,
            "rows": len(rows),
            "required_cols_ok": False,
            "errors": ["file_missing_or_unreadable"] if not path.exists() or not json_ok else ["rows_not_exact"],
        }
        info = {
            "exists": bool(path.exists()),
            "json_ok": bool(json_ok),
            "rows": int(result.get("rows") or 0),
            "required_cols_ok": bool(result.get("required_cols_ok")),
            "strict_quality_ok": bool(result.get("ok")),
            "errors": list(result.get("errors") or []),
        }
        logger.info("[TRADE][FINAL30][FILE_REPAIR][POST_VALIDATE] label=%s result=%s", label, info)
        path_results[label] = info
        aggregate_ok = aggregate_ok and bool(info["strict_quality_ok"])
        aggregate_errors.extend(info["errors"])
    logger.info(
        "[TRADE][FINAL30][STRICT_VALIDATE][FILES] ok=%s errors=%s",
        int(aggregate_ok),
        list(dict.fromkeys(aggregate_errors)),
    )
    return {
        "ok": aggregate_ok,
        "errors": list(dict.fromkeys(aggregate_errors)),
        "paths": path_results,
    }


def _strict_validate_trade_final30_rows(rows: list[dict[str, Any]], *, source: str) -> dict[str, Any]:
    summary = summarize_entry_style_distribution(rows)
    logger.info(
        "[TRADE][FINAL30][ENTRY_STYLE_DISTRIBUTION] total=%s counts=%s",
        int(summary.get("total") or 0),
        dict(summary.get("counts") or {}),
    )
    return verify_final30_scored_rows(
        rows,
        required_rows=30,
        required_fields=REQUIRED_FINAL30_SCORED_COLS,
        source=source,
    )


def _trade_min_tradeable_candidates() -> int:
    try:
        return max(1, int(os.getenv("PB1_MIN_TRADEABLE_CANDIDATES", os.getenv("MIN_TRADEABLE_CANDIDATES", "10"))))
    except Exception:
        return 10


def _positive_ma20_count(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in (rows or [])
        if safe_nullable_float((row or {}).get("ma20")) is not None and float((row or {}).get("ma20") or 0.0) > 0
    )


def _trade_recheck_rows(rows: list[dict[str, Any]], *, source: str) -> dict[str, Any]:
    return verify_final30_scored_rows(
        rows,
        required_rows=len(rows),
        required_fields=REQUIRED_FINAL30_SCORED_COLS,
        source=source,
    )


def _is_trade_repairable_db_contract(summary: dict[str, Any], *, missing_critical_fields: list[str] | None = None) -> bool:
    errors = set(summary.get("errors") or [])
    allowed_errors = {"ma20_invalid_rows"}
    return (
        int(summary.get("rows") or 0) == 30
        and int(summary.get("uniq_codes") or 0) == 30
        and not list(missing_critical_fields or [])
        and bool(errors)
        and errors.issubset(allowed_errors)
    )


def _fetch_trade_ohlcv_df(code: str) -> pd.DataFrame:
    try:
        result = KRXOHLCVProvider().get_ohlcv(
            str(code).zfill(6),
            30,
            purpose="final30_contract_repair",
            usage_context="trade",
        )
        frame = result.df if hasattr(result, "df") else result
        return frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    except Exception as exc:
        logger.warning("[TRADE][FINAL30][REPAIR][OHLCV_FAIL] code=%s err=%s", str(code).zfill(6), exc)
        return pd.DataFrame()


def _repair_trade_final30_rows(
    *,
    engine,
    env: str,
    as_of: str,
    rows: list[dict[str, Any]],
    repo_root_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    repo = WatchlistRepo(engine)
    as_of_date = datetime.strptime(str(as_of), "%Y-%m-%d").date()
    universe_rows, _ = repo.load_watchlist_scored(
        env=env,
        strategy="pb1_universe_scored",
        as_of=as_of_date,
        allow_latest_fallback=False,
    )
    top50_rows, _ = repo.load_watchlist(
        env=env,
        strategy="pb1_top50",
        as_of=as_of_date,
        allow_latest_fallback=False,
    )
    runtime_rows: list[dict[str, Any]] = []
    for label, path in build_final30_paths(repo_root_path, env, as_of).items():
        file_rows, json_ok = read_final30_file_rows(path)
        if json_ok and file_rows:
            runtime_rows.extend(file_rows)
    try:
        derived_rows = DerivedMinerviniRepo(engine).load_for_as_of(
            env=env,
            as_of=as_of_date,
            symbols=[str((row or {}).get("code") or "").zfill(6) for row in rows if (row or {}).get("code")],
        )
    except Exception:
        derived_rows = []
    reference_maps = []
    for source_rows in (rows, universe_rows, top50_rows, runtime_rows):
        reference_maps.append({
            str((row or {}).get("code") or "").zfill(6): normalize_final30_contract_row(dict(row or {}))
            for row in (source_rows or [])
            if (row or {}).get("code")
        })
    derived_map = {str((row or {}).get("symbol") or "").zfill(6): dict(row or {}) for row in derived_rows}

    repaired_rows: list[dict[str, Any]] = []
    repaired = 0
    unresolved = 0
    for raw in rows:
        row = normalize_final30_contract_row(dict(raw or {}))
        code = str((row or {}).get("code") or "").zfill(6)
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        scores = row.get("scores") if isinstance(row.get("scores"), dict) else {}
        if safe_nullable_float(row.get("ma20")) is None or float(row.get("ma20") or 0.0) <= 0:
            candidate_sources: list[dict[str, Any]] = [row, meta, scores]
            for ref_map in reference_maps:
                ref = ref_map.get(code)
                if ref:
                    candidate_sources.append(ref)
                    if isinstance(ref.get("meta"), dict):
                        candidate_sources.append(ref.get("meta"))
            derived_row = derived_map.get(code, {})
            if derived_row:
                candidate_sources.append(derived_row)
                if isinstance(derived_row.get("features_json"), dict):
                    candidate_sources.append(derived_row.get("features_json"))
            repaired_ma20 = None
            for source in candidate_sources:
                if not isinstance(source, dict):
                    continue
                for key in ("ma20", "ma_20", "sma20", "ma20_price"):
                    repaired_ma20 = safe_nullable_float(source.get(key))
                    if repaired_ma20 is not None and float(repaired_ma20) > 0:
                        break
                if repaired_ma20 is not None and float(repaired_ma20) > 0:
                    break
            if (repaired_ma20 is None or float(repaired_ma20) <= 0) and code:
                ohlcv_df = _fetch_trade_ohlcv_df(code)
                if not ohlcv_df.empty:
                    repaired_ma20 = compute_ma20_from_ohlcv(ohlcv_df)
            if repaired_ma20 is not None and float(repaired_ma20) > 0:
                row["ma20"] = float(repaired_ma20)
                meta["ma20"] = float(repaired_ma20)
                row["meta"] = meta
                repaired += 1
            else:
                unresolved += 1
        repaired_rows.append(normalize_final30_contract_row(row))
    return repaired_rows, {"repaired": repaired, "unresolved": unresolved}


def _select_repaired_final30_source(
    *,
    repo_root_path: Path,
    env: str,
    as_of: str,
    db_rows: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    for label, path in build_final30_paths(repo_root_path, env, as_of).items():
        rows, json_ok = read_final30_file_rows(path)
        if not json_ok or not rows:
            continue
        validate = _strict_validate_trade_final30_rows(rows, source=f"FILE:{label}")
        if bool(validate.get("ok")):
            logger.info("[TRADE][FINAL30][SOURCE_RESELECT] selected=%s", label)
            logger.info(
                "[TRADE][FINAL30][POST_REPAIR_LOAD] rows=%s ma20_positive=%s",
                len(rows),
                _positive_ma20_count(rows),
            )
            return label, rows
    logger.info("[TRADE][FINAL30][SOURCE_RESELECT] selected=db")
    logger.info(
        "[TRADE][FINAL30][POST_REPAIR_LOAD] rows=%s ma20_positive=%s",
        len(db_rows),
        _positive_ma20_count(db_rows),
    )
    return "db", db_rows


def _classify_final30_abort_reason(*, rows: int, missing_cols: list[str], errors: list[str] | None = None) -> str:
    error_set = {str(error) for error in (errors or []) if str(error).strip()}
    if "strategy_mismatch" in error_set:
        return "strategy_mismatch"
    if int(rows) == 0:
        return "scored_final30_missing"
    if int(rows) != 30:
        return "rows_not_30"
    if list(missing_cols or []):
        return "required_scored_cols_missing"
    return "missing_locked_scored_final30"


def _empty_final30_load_result(*, final30_paths: dict[str, Path], missing_scored_cols: list[str], reason: str) -> dict[str, Any]:
    return {
        "df": pd.DataFrame(),
        "source_name": "none",
        "as_of": None,
        "columns": [],
        "is_scored": False,
        "used_fallback": False,
        "file_mirror_present": False,
        "usable": False,
        "missing_scored_cols": list(missing_scored_cols),
        "flow_optional_missing": [],
        "path_map": {key: str(value) for key, value in final30_paths.items()},
        "source_compare": {},
        "abort_reason": str(reason),
    }


def _log_final30_fallback_blocked() -> None:
    logger.error("[FINAL30][FALLBACK_BLOCKED] from=pb1_watchlist_final_scored to=best_k_meta")
    logger.error("[FINAL30][FALLBACK_BLOCKED] from=pb1_watchlist_final_scored to=candidate_pool")
    logger.error("[FINAL30][FALLBACK_BLOCKED] from=pb1_watchlist_final_scored to=file_mirror")


def _db_exact_scored_final30_abort_reason(exc: Exception | None = None) -> str:
    if isinstance(exc, ScoredWatchlistNotFoundError):
        if int(getattr(exc, "rows", 0) or 0) == 0:
            return "db_exact_scored_zero_rows"
        return "db_exact_scored_not_loaded"
    if isinstance(exc, ScoredWatchlistInvalidError):
        reason = str(getattr(exc, "reason", "") or "")
        if reason == "rows_not_30":
            return "db_exact_scored_bad_rowcount"
        if list(getattr(exc, "missing_cols", []) or []):
            return "db_exact_scored_missing_critical_cols"
    return "db_exact_scored_not_loaded"


def _raise_entry_abort_precheck(reason: str) -> None:
    raise RuntimeError(f"ENTRY_ABORT_PRECHECK:{reason}")


def _structured_entry_abort(reason: str, *, expected_as_of: str | None = None, db_rows: int | None = None) -> tuple[list[Path], bool, dict[str, int], str, str]:
    logger.error("[PB1][ENTRY][ABORT] reason=%s expected_as_of=%s db_rows=%s", reason, expected_as_of or "", "" if db_rows is None else db_rows)
    logger.info("[RUN_SUMMARY][RESULT] status=FAIL_PRECHECK reason=DB_EXACT_FINAL30_ZERO orders_intent=0 orders_ack=0")
    return [], False, {}, "entry", "FAIL_PRECHECK"


ALLOWED_FINAL30_SOURCES = {
    "db_pb1_watchlist_final_scored",
    "kr_canonical_artifact",
    "db_exact_fallback",
    "prep.db_roundtrip",
    "trade.inject.artifact",
    "trade.inject.db_fallback",
}


def _validate_trade_locked_final30_or_raise(
    *,
    final30_df: pd.DataFrame | None,
    source_name: str,
    as_of: str | None = None,
    env: str | None = None,
    allow_empty_for_close_exit: bool = False,
) -> None:
    source_name = str(source_name or "none")
    frame = final30_df if isinstance(final30_df, pd.DataFrame) else pd.DataFrame()
    if frame.empty:
        if allow_empty_for_close_exit:
            logger.warning(
                "[TRADE][FINAL30][LOCK][BYPASS_FOR_CLOSE_EXIT] source=%s reason=empty_allowed_for_liquidation",
                source_name,
            )
            return
        logger.error("[TRADE][FINAL30][LOCK][FAIL] reason=empty_precomputed_final30 source=%s", source_name)
        _raise_entry_abort_precheck("db_exact_scored_zero_rows")
    if source_name not in ALLOWED_FINAL30_SOURCES:
        logger.error(
            "[TRADE][FINAL30][LOCK][FAIL] reason=unsupported_source source=%s allowed=%s",
            source_name,
            sorted(ALLOWED_FINAL30_SOURCES),
        )
        _raise_entry_abort_precheck("db_exact_scored_not_loaded")
    try:
        _rows, info = assert_final30_contract(
            frame,
            as_of=as_of,
            env=env,
            source=f"trade.lock.{source_name}",
            require_count=30,
        )
    except Exception as exc:
        logger.exception("[TRADE][FINAL30][LOCK][FAIL] reason=contract_fail source=%s err=%s", source_name, exc)
        _raise_entry_abort_precheck("db_exact_scored_contract_fail")
    logger.info("[TRADE][FINAL30][LOCK][OK] source=%s rows=%s hash=%s", source_name, info.get("rows"), info.get("contract_hash"))


def _forced_close_live_execution_enabled() -> bool:
    forced_window = (os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
    forced_phase = ((os.getenv("FORCE_PB1_PHASE") or os.getenv("PB1_PHASE_DEFAULT") or "").strip().lower())
    return bool(
        forced_window == "close"
        and forced_phase == "exit"
        and (os.getenv("STRATEGY_MODE") or "").strip().upper() == "LIVE"
        and (os.getenv("FORCE_STRATEGY_MODE") or "").strip().upper() == "LIVE"
        and parse_bool_any(os.getenv("DRY_RUN"), default=True) is False
        and parse_bool_any(os.getenv("DISABLE_LIVE_TRADING"), default=False) is False
        and parse_bool_any(os.getenv("LIVE_TRADING_ENABLED"), default=False) is True
        and parse_bool_any(os.getenv("FORCE_BLOCK_LIVE"), default=False) is False
    )


def _resolve_close_manual_mode() -> str:
    raw_mode = str(os.getenv("CLOSE_MANUAL_MODE") or "live_close").strip().lower()
    if raw_mode in {"live_close", "diag_replay", "compute_only"}:
        return raw_mode
    logger.warning("[TRADE_CLOSE][MANUAL_MODE][INVALID] raw=%s fallback=live_close", raw_mode)
    return "live_close"


def _is_close_manual_replay_requested() -> bool:
    return bool(
        _resolve_session_kind() == "close"
        and str(os.getenv("GITHUB_EVENT_NAME") or "").strip().lower() == "workflow_dispatch"
        and _resolve_close_manual_mode() in {"diag_replay", "compute_only"}
    )


def _is_close_manual_replay_active() -> bool:
    return os.getenv("PB1_CLOSE_MANUAL_REPLAY_ACTIVE", "0") == "1"


def _activate_close_manual_replay_env() -> str | None:
    if not _is_close_manual_replay_requested():
        return None
    mode = _resolve_close_manual_mode()
    os.environ["PB1_CLOSE_MANUAL_REPLAY_ACTIVE"] = "1"
    os.environ["PB1_CLOSE_MANUAL_REPLAY_MODE"] = mode
    os.environ["PB1_ENTRY_ENABLED"] = "0"
    os.environ["FORCE_MARKET_WINDOW"] = "close"
    os.environ["FORCE_PB1_PHASE"] = "exit"
    os.environ["FORCE_BLOCK_LIVE"] = "1"
    os.environ["DISABLE_LIVE_TRADING"] = "1"
    os.environ["LIVE_TRADING_ENABLED"] = "0"
    os.environ["STRATEGY_MODE"] = "DIAG"
    os.environ["FORCE_STRATEGY_MODE"] = "DIAG"
    if mode == "compute_only":
        os.environ["DRY_RUN"] = "1"
    logger.info(
        "[TRADE_CLOSE][MANUAL_REPLAY][ARM] mode=%s dry_run=%s force_block_live=%s disable_live=%s",
        mode,
        os.getenv("DRY_RUN"),
        os.getenv("FORCE_BLOCK_LIVE"),
        os.getenv("DISABLE_LIVE_TRADING"),
    )
    return mode


def _pm_should_handoff_to_close(now: datetime) -> bool:
    forced_window = (os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
    forced_phase = (os.getenv("FORCE_PB1_PHASE") or "").strip().lower()
    if forced_window != "day" or forced_phase != "entry":
        return False
    close_start = _parse_hhmm_to_time(CLOSE_AUCTION_START)
    return now.time() >= close_start


def _after_close_entry_dryrun_enabled(now: datetime) -> bool:
    forced_window = (os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
    forced_phase = (os.getenv("FORCE_PB1_PHASE") or "").strip().lower()
    mode_input = (os.getenv("MODE") or "").strip().lower()
    strategy_mode = (os.getenv("STRATEGY_MODE") or "").strip().upper()
    allow_flag = parse_bool_any(os.getenv("PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN"), default=False)
    force_run = parse_bool_any(os.getenv("FORCE_RUN"), default=False)
    diag_full_exec = parse_bool_any(os.getenv("PB1_DIAG_FULL_EXEC"), default=False)
    dry_run = parse_bool_any(os.getenv("DRY_RUN"), default=True)
    force_block_live = parse_bool_any(os.getenv("FORCE_BLOCK_LIVE"), default=False)
    close_start = _parse_hhmm_to_time(CLOSE_AUCTION_START)

    # FORCE_RUN=1 + PB1_DIAG_FULL_EXEC=1 조합도 allow arm으로 인정 (PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN 없이도 동작)
    arm = allow_flag or (force_run and diag_full_exec)
    base = bool(
        mode_input == "trade"
        and strategy_mode == "DIAG"
        and forced_window == "day"
        and forced_phase == "entry"
        and dry_run is True
        and force_block_live is True
        and now.time() >= close_start
    )
    return bool(arm and base)


def _hydrate_locked_final30_from_db_only(*, engine, env: str, as_of: date | str) -> pd.DataFrame:
    strategy_key = os.getenv("WATCHLIST_FINAL_SCORED_STRATEGY_KEY", os.getenv("WATCHLIST_FINAL_STRATEGY_KEY", "pb1_watchlist_final_scored")).strip().lower()
    os.environ["PB1_LAST_STAGE"] = "final30.db_load.start"
    logger.info("[PB1][STAGE][START] stage=final30.db_load env=%s as_of=%s strategy=%s", env, as_of, strategy_key)
    try:
        df = load_final30_scored_exact(
            engine,
            env=env,
            as_of=as_of,
            strategy=strategy_key,
            require_rows=30,
            fail_if_missing=True,
        )
    except (ScoredWatchlistNotFoundError, ScoredWatchlistInvalidError) as exc:
        reason = _db_exact_scored_final30_abort_reason(exc)
        logger.exception("[FINAL30][DB_LOAD][FAIL] err_type=%s err=%s", type(exc).__name__, exc)
        logger.error("[TRADE][FINAL30][DB_ONLY_LOCK][FAIL] env=%s as_of=%s reason=%s", env, as_of, reason)
        os.environ["PB1_LAST_STAGE"] = "final30.db_load.fail"
        raise RuntimeError(f"ENTRY_ABORT_PRECHECK:{reason}") from exc

    os.environ["PB1_LAST_STAGE"] = "final30.db_load.done"
    logger.info("[PB1][STAGE][END] stage=final30.db_load rows=%s", len(df))
    logger.info(
        "[TRADE][FINAL30][DB_ONLY_LOCK] env=%s as_of=%s rows=%s source=db_pb1_watchlist_final_scored",
        env,
        as_of,
        len(df),
    )
    return df


def load_locked_final30_from_db(
    *,
    engine,
    env: str,
    derived_as_of: str,
    strategy_key: str = "pb1_watchlist_final_scored",
) -> dict[str, Any]:
    env_n = (env or "").strip().lower()
    logger.info(
        "[FINAL30][DB_LOCK][START] env=%s as_of=%s strategy=%s",
        env_n,
        derived_as_of,
        strategy_key,
    )
    df = _hydrate_locked_final30_from_db_only(engine=engine, env=env_n, as_of=str(derived_as_of))
    rows = len(df)
    codes = [str(code).zfill(6) for code in df.get("code", pd.Series(dtype=str)).tolist() if str(code).strip()]
    result = {
        "df": df,
        "source_name": "db_pb1_watchlist_final_scored",
        "as_of": str(derived_as_of),
        "columns": [str(col) for col in df.columns.tolist()],
        "is_scored": True,
        "used_fallback": False,
        "file_mirror_present": False,
        "usable": True,
        "missing_scored_cols": [],
        "flow_optional_missing": _safe_flow_optional_missing([str(col) for col in df.columns.tolist()]),
        "path_map": {},
        "source_compare": {},
        "locked": True,
        "rows": rows,
        "codes": codes,
    }
    logger.info(
        "[FINAL30][DB_LOCK][OK] rows=%s codes=%s source=%s",
        rows,
        len(codes),
        result.get("source_name"),
    )
    return result


def _assert_engine_boot_locked_final30(*, run_ctx: dict[str, Any], final30_df: pd.DataFrame | None) -> None:
    frame = final30_df if isinstance(final30_df, pd.DataFrame) else pd.DataFrame()
    rows = len(frame)
    source = str(run_ctx.get("final30_source") or "none")
    locked = bool(run_ctx.get("final30_locked"))
    missing_cols = [col for col in FINAL30_SCORED_DB_CONTRACT_FIELDS if col not in set(str(c) for c in frame.columns.tolist())]
    source_allowed_for_boot = source in ALLOWED_FINAL30_SOURCES or source in {"canonical", "injected_canonical"}
    if not locked or not source_allowed_for_boot or rows == 0:
        reason = "db_exact_scored_zero_rows" if rows == 0 else "db_exact_scored_not_loaded"
        logger.error("[PB1][ENTRY][ABORT] reason=%s", reason)
        _raise_entry_abort_precheck(reason)
    if rows != 30 or missing_cols:
        reason = "db_exact_scored_bad_rowcount" if rows != 30 else "db_exact_scored_missing_critical_cols"
        logger.error("[PB1][ENTRY][ABORT] reason=%s", reason)
        _raise_entry_abort_precheck(reason)
    boot_rows, boot_info = assert_final30_contract(frame, as_of=str(run_ctx.get("derived_as_of") or run_ctx.get("as_of") or ""), env=str(run_ctx.get("env") or os.getenv("KIS_ENV") or "practice"), source="trade.engine_boot", require_count=30)
    frame = pd.DataFrame(boot_rows)
    logger.info(
        "[TRADE][ENGINE_BOOT][FINAL30_OK] rows=%s rank_min=%s rank_max=%s unique=%s hash=%s",
        len(frame), int(frame["rank_final30"].min()), int(frame["rank_final30"].max()), int(frame["rank_final30"].nunique()), boot_info["contract_hash"],
    )


def _load_kr_injected_final30_df(*, trade_date: str, expected_as_of: str, env: str, engine=None) -> pd.DataFrame:
    if str(os.getenv("KR_INJECT_CANONICAL_FINAL30", "1")).strip().lower() in {"0", "false", "no"}:
        return pd.DataFrame()
    strategy = os.getenv("PB1_FINAL30_STRATEGY_KEY") or os.getenv("WATCHLIST_FINAL_SCORED_STRATEGY_KEY") or "pb1_watchlist_final_scored"
    artifact_exc: Exception | None = None
    try:
        from trader.kr.artifacts import validate_kr_prep_artifact
        art = validate_kr_prep_artifact(
            trade_date=date.fromisoformat(str(trade_date)[:10]),
            expected_as_of=date.fromisoformat(str(expected_as_of)[:10]),
            env=env,
            strict=True,
            allow_legacy_fallback=False,
        )
        if not art.ok:
            raise RuntimeError(f"KR_ARTIFACT_NOT_OK:{art.reason}")
        rows = list(getattr(art, "final30_rows_payload", []) or [])
        normalized_rows, info = assert_final30_contract(rows, as_of=str(expected_as_of)[:10], env=env, source="trade.inject.artifact", require_count=30)
        df = pd.DataFrame(normalized_rows)
        df.attrs["final30_context"] = {"source": "kr_canonical_artifact", "as_of": str(expected_as_of)[:10], "rows": 30, "locked": 1, "usable": 1, "is_scored": 1, "contract_ok": 1, "strategy": strategy, "contract_hash": info["contract_hash"], "validated_by": "final30_contract"}
        logger.info("[KR_FINAL30][INJECT][OK] source=artifact rows=%s hash=%s", len(normalized_rows), info["contract_hash"])
        logger.info("[KR_FINAL30][CONTEXT] source=kr_canonical_artifact rows=%s hash=%s", len(normalized_rows), info["contract_hash"])
        return df
    except Exception as exc:
        artifact_exc = exc
        logger.exception("[KR_FINAL30][INJECT][ARTIFACT_FAIL] err=%s -> trying_db_fallback=1", exc)

    try:
        if engine is None:
            raise RuntimeError("DB_ENGINE_MISSING_FOR_FINAL30_FALLBACK")
        from trader.db.repos import load_exact_final30_scored
        db_rows = load_exact_final30_scored(engine, env=env, as_of=str(expected_as_of)[:10], strategy=strategy)
        normalized_rows, info = assert_final30_contract(db_rows, as_of=str(expected_as_of)[:10], env=env, source="trade.inject.db_fallback", require_count=30)
        df = pd.DataFrame(normalized_rows)
        df.attrs["final30_context"] = {"source": "db_exact_fallback", "as_of": str(expected_as_of)[:10], "rows": 30, "locked": 1, "usable": 1, "is_scored": 1, "contract_ok": 1, "strategy": strategy, "contract_hash": info["contract_hash"], "validated_by": "final30_contract"}
        logger.warning("[KR_FINAL30][INJECT][FALLBACK_DB_OK] rows=%s hash=%s", len(normalized_rows), info["contract_hash"])
        logger.info("[KR_FINAL30][CONTEXT] source=db_exact_fallback rows=%s hash=%s", len(normalized_rows), info["contract_hash"])
        return df
    except Exception as db_exc:
        logger.exception("[KR_FINAL30][INJECT][FALLBACK_DB_FAIL] err=%s", db_exc)
        raise RuntimeError(f"KR_FINAL30_INJECT_FAIL artifact_err={artifact_exc} db_err={db_exc}")


def _holding_code_qty(holding: dict[str, Any]) -> tuple[str, int]:
    code = str((holding or {}).get("code") or (holding or {}).get("pdno") or (holding or {}).get("symbol") or "").strip()
    if code and code.isdigit():
        code = code.zfill(6)
    try:
        qty = int(float((holding or {}).get("qty") or (holding or {}).get("hldg_qty") or (holding or {}).get("quantity") or 0))
    except Exception:
        qty = 0
    return code, qty


def _is_accepted_order_response(resp: Any) -> bool:
    if not isinstance(resp, dict):
        return False
    if str(resp.get("rt_cd") or "") == "0":
        return True
    status = str(resp.get("status") or resp.get("result") or "").strip().upper()
    return status in {"ACCEPTED", "SUBMITTED", "OK"}


def _kr_close_liquidation_explicitly_confirmed() -> bool:
    return (
        str(os.getenv("KR_CLOSE_LIQUIDATION_ALL_ENABLED", "0")).strip().lower() in {"1", "true", "yes"}
        and os.getenv("KR_CLOSE_LIQUIDATION_ALL_CONFIRM") == "RUN_KR_CLOSE_LIQUIDATION_ALL"
    )


def _parse_meta_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


KR_CLOSE_METADATA_DEFAULTS = {
    "position_book": "SWING_BOOK",
    "trade_horizon": "SWING_CARRY",
    "exit_policy_family": "SWING_STAGED_EXIT",
    "eod_action": "CARRY_IF_NO_EXIT_SIGNAL",
    "force_eod_close": False,
}


def _coerce_bool_meta(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _extract_tagged_close_metadata(
    code: str,
    *,
    db_positions: list[dict[str, Any]] | None,
    latest_buy_fills: list[dict[str, Any]] | None,
) -> tuple[dict[str, Any], str]:
    code = str(code or "").zfill(6)
    for source_name, rows, meta_fields in (
        ("position_meta", db_positions or [], ("position_meta", "position_meta_json", "meta", "meta_json")),
        ("fill_meta", latest_buy_fills or [], ("fill_meta_json", "entry_meta_json", "meta", "meta_json")),
    ):
        for row in rows:
            row_code = str((row or {}).get("code") or (row or {}).get("pdno") or "").zfill(6)
            if row_code != code:
                continue
            meta: dict[str, Any] = {}
            for key in meta_fields:
                meta.update(_parse_meta_json((row or {}).get(key)))
            for key in KR_CLOSE_METADATA_DEFAULTS:
                if (row or {}).get(key) not in (None, ""):
                    meta[key] = (row or {}).get(key)
            if meta:
                return meta, source_name
    return {}, "metadata_missing"


def build_kr_close_policy_orders_from_tagged_positions(
    kis_holdings: list[dict[str, Any]] | None,
    db_positions: list[dict[str, Any]] | None = None,
    latest_buy_fills: list[dict[str, Any]] | None = None,
    orders_repo: Any = None,
    fills_repo: Any = None,
    positions_repo: Any = None,
    market_state_overlay: Any = None,
) -> list[dict[str, Any]]:
    """Build KR close SELL orders only when tagged DAY/SWING/CORE policy allows it.

    Missing metadata is intentionally conservative: fallback to SWING_CARRY/HOLD.
    """
    orders: list[dict[str, Any]] = []
    for holding in list(kis_holdings or []):
        code, qty = _holding_code_qty(holding)
        if not code or qty <= 0:
            continue
        meta, metadata_source = _extract_tagged_close_metadata(
            code,
            db_positions=db_positions,
            latest_buy_fills=latest_buy_fills,
        )
        missing = not bool(meta)
        merged = dict(KR_CLOSE_METADATA_DEFAULTS)
        merged.update(meta)
        book = str(merged.get("position_book") or "").upper()
        horizon = str(merged.get("trade_horizon") or "").upper()
        exit_family = str(merged.get("exit_policy_family") or "").upper()
        eod_action = str(merged.get("eod_action") or "").upper()
        close_action = str(merged.get("close_action") or "").upper()
        force_eod = _coerce_bool_meta(merged.get("force_eod_close"))
        action = "HOLD"
        reason = "KR_CLOSE_HOLD_METADATA_MISSING" if missing else "KR_CLOSE_HOLD_SWING_CARRY"
        if missing:
            logger.info("[KR_CLOSE][POLICY][METADATA_MISSING] code=%s action=HOLD fallback=SWING_CARRY", code)
        is_day = book == "DAY_BOOK" or horizon in {"DAY_TRADE", "DAY_PROTECT"}
        is_core = book == "CORE_BOOK" or horizon in {"CORE", "CORE_CARRY"}
        if is_day and (force_eod or eod_action in {"FORCE_EXIT", "CLOSE", "EOD_CLOSE"} or close_action == "FORCE_SELL"):
            action = "SELL"
            reason = "KR_CLOSE_DAY_FORCE_EOD"
        elif is_core:
            reason = "KR_CLOSE_HOLD_CORE_CARRY"
        elif not missing:
            reason = "KR_CLOSE_HOLD_SWING_CARRY"
        logger.info(
            "[KR_CLOSE][POLICY][EVAL] code=%s book=%s horizon=%s force_eod=%s action=%s reason=%s",
            code, book, horizon, int(force_eod), action, reason,
        )
        if action == "SELL":
            orders.append({
                "code": code,
                "qty": qty,
                "side": "SELL",
                "reason": reason,
                "source": "tagged_close_policy",
                "position_book": book,
                "trade_horizon": horizon,
                "exit_policy_family": exit_family,
                "force_eod_close": force_eod,
                "eod_action": eod_action,
                "metadata_source": metadata_source,
            })
    return orders


def _load_kr_close_tagged_metadata(
    *,
    holdings: list[dict[str, Any]],
    positions_repo: Any,
    fills_repo: Any,
    orders_repo: Any,
    env: str,
    strategy: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    codes = [code for code, qty in (_holding_code_qty(row) for row in (holdings or [])) if code and qty > 0]
    db_positions: list[dict[str, Any]] = []
    latest_buy_fills: list[dict[str, Any]] = []
    if positions_repo is not None:
        if hasattr(positions_repo, "list_positions_by_codes"):
            db_positions = list(positions_repo.list_positions_by_codes(env=env, strategy=strategy, codes=codes) or [])
        elif hasattr(positions_repo, "list_positions"):
            all_positions = list(positions_repo.list_positions(env, strategy) or [])
            code_set = set(codes)
            db_positions = [dict(row) for row in all_positions if str((row or {}).get("code") or "").zfill(6) in code_set]
    latest_by_code: dict[str, dict[str, Any]] = {}
    if fills_repo is not None and hasattr(fills_repo, "list_latest_buy_fills_by_codes"):
        raw_latest = fills_repo.list_latest_buy_fills_by_codes(env, codes) or {}
        if isinstance(raw_latest, dict):
            latest_by_code = {str(code).zfill(6): dict(row or {}) for code, row in raw_latest.items()}
    elif fills_repo is not None and hasattr(fills_repo, "list_today_fills"):
        for code in codes:
            rows = fills_repo.list_today_fills(env, side="BUY", code=code) or []
            if rows:
                latest_by_code[code] = dict(rows[0] or {})
    if orders_repo is not None:
        for code in codes:
            if latest_by_code.get(code) and _parse_meta_json(latest_by_code[code].get("fill_meta_json")):
                continue
            entry_meta: dict[str, Any] = {}
            if hasattr(orders_repo, "list_today_buy_orders"):
                buy_orders = orders_repo.list_today_buy_orders(env, code=code) or []
                if buy_orders:
                    entry_meta = _parse_meta_json((buy_orders[0] or {}).get("entry_meta_json"))
            if not entry_meta and hasattr(orders_repo, "find_latest_buy_entry_exit_plan"):
                plan_row = orders_repo.find_latest_buy_entry_exit_plan(env, strategy, code) or {}
                entry_meta = _parse_meta_json(plan_row.get("entry_meta"))
            if entry_meta:
                latest_by_code.setdefault(code, {"code": code})
                latest_by_code[code]["entry_meta_json"] = entry_meta
    latest_buy_fills = list(latest_by_code.values())
    logger.info(
        "[KR_CLOSE][POLICY][DB_METADATA] holdings=%s db_positions=%s latest_buy_fills=%s codes=%s",
        len(codes), len(db_positions), len(latest_buy_fills), ",".join(codes[:20]),
    )
    return db_positions, latest_buy_fills


def submit_kr_close_policy_orders(
    *,
    policy_orders: list[dict[str, Any]],
    kis_client: Any,
    orders_repo: Any = None,
    env: str = "practice",
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for order in list(policy_orders or []):
        code = str((order or {}).get("code") or "").zfill(6)
        qty = int((order or {}).get("qty") or 0)
        if not code or qty <= 0:
            continue
        reason = str((order or {}).get("reason") or "KR_CLOSE_POLICY_SELL")
        logger.info("[KR_CLOSE][POLICY][SELL][INTENT] code=%s qty=%s reason=%s metadata_source=%s", code, qty, reason, (order or {}).get("metadata_source"))
        if dry_run:
            results.append({**dict(order), "accepted": False, "dry_run": True, "result": "DRY_RUN"})
            continue
        if kis_client is None or not hasattr(kis_client, "sell_stock_market"):
            raise RuntimeError("KIS_SELL_SUBMIT_UNAVAILABLE_FOR_KR_CLOSE_POLICY")
        logger.info("[ORDER][API_CALL][START] side=SELL code=%s source=tagged_close_policy", code)
        resp = kis_client.sell_stock_market(code, qty)
        ok = _is_accepted_order_response(resp)
        rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
        msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
        msg1 = resp.get("msg1") if isinstance(resp, dict) else None
        logger.info("[KIS][ORDER][RESPONSE] side=SELL code=%s rt_cd=%s msg_cd=%s msg1=%s source=tagged_close_policy", code, rt_cd, msg_cd, msg1)
        logger.info("[TRADE][ORDER][SELL] code=%s result=%s source=tagged_close_policy", code, "ACCEPTED" if ok else "REJECTED")
        result = dict(resp) if isinstance(resp, dict) else {"resp": resp}
        result.update({**dict(order), "accepted": ok, "result": "ACCEPTED" if ok else "REJECTED"})
        results.append(result)
    return results


def run_kr_close_policy_from_tagged_positions(
    *,
    kis_holdings: list[dict[str, Any]],
    positions_repo: Any,
    fills_repo: Any,
    orders_repo: Any,
    kis_client: Any,
    env: str,
    strategy: str = "pb1_pullback_close",
    dry_run: bool = False,
) -> dict[str, Any]:
    db_positions, latest_buy_fills = _load_kr_close_tagged_metadata(
        holdings=kis_holdings,
        positions_repo=positions_repo,
        fills_repo=fills_repo,
        orders_repo=orders_repo,
        env=env,
        strategy=strategy,
    )
    policy_orders = build_kr_close_policy_orders_from_tagged_positions(
        kis_holdings,
        db_positions=db_positions,
        latest_buy_fills=latest_buy_fills,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
    )
    policy_results = submit_kr_close_policy_orders(
        policy_orders=policy_orders,
        kis_client=kis_client,
        orders_repo=orders_repo,
        env=env,
        dry_run=dry_run,
    )
    accepted_policy_sells = sum(1 for row in policy_results if bool((row or {}).get("accepted")))
    return {
        "policy_orders": policy_orders,
        "policy_results": policy_results,
        "accepted_policy_sells": accepted_policy_sells,
        "db_positions": db_positions,
        "latest_buy_fills": latest_buy_fills,
        "policy_sell_candidates": len(policy_orders),
    }


def run_close_liquidation_from_kis_holdings(
    *,
    kis_client: Any,
    orders_repo: Any = None,
    env: str = "practice",
    holdings: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
    submit_sell_order: Any = None,
) -> list[dict[str, Any]]:
    """Submit emergency close-liquidation SELLs using KIS holdings as source of truth."""
    if not _kr_close_liquidation_explicitly_confirmed():
        logger.error("[KR_CLOSE][LIQUIDATION][BLOCKED] reason=MISSING_EXPLICIT_CONFIRM")
        return []
    if holdings is None:
        logger.warning("[KR_CLOSE][LIQUIDATION][BALANCE_RELOAD][START] source=kis_client")
        raw = {}
        if kis_client is not None and hasattr(kis_client, "get_balance"):
            raw = kis_client.get_balance() or {}
        elif kis_client is not None and hasattr(kis_client, "inquire_balance"):
            raw = kis_client.inquire_balance() or {}
        if isinstance(raw, dict):
            holdings = list(raw.get("output1") or raw.get("holdings") or [])
        else:
            holdings = []
        logger.warning("[KR_CLOSE][LIQUIDATION][BALANCE_RELOAD][DONE] holdings=%s", len(holdings or []))
    rows = list(holdings or [])
    if not rows:
        logger.info("[KR_CLOSE][LIQUIDATION][SKIP] reason=NO_KIS_HOLDINGS")
        return []
    logger.warning("[KR_CLOSE][LIQUIDATION][START] source=kis_holdings holdings=%s", len(rows))
    results: list[dict[str, Any]] = []
    for holding in rows:
        code, qty = _holding_code_qty(holding)
        if not code or qty <= 0:
            continue
        logger.warning("[KR_CLOSE][SELL][INTENT] code=%s qty=%s reason=KR_CLOSE_LIQUIDATION_KIS_HOLDING", code, qty)
        if dry_run:
            results.append({"side": "SELL", "code": code, "qty": qty, "reason": "KR_CLOSE_LIQUIDATION_KIS_HOLDING", "source": "kis_holdings", "dry_run": True})
            continue
        logger.info("[ORDER][API_CALL][START] side=SELL code=%s", code)
        if submit_sell_order is not None:
            resp = submit_sell_order(
                kis_client=kis_client,
                orders_repo=orders_repo,
                env=env,
                code=code,
                qty=qty,
                order_type="MARKET",
                reason="KR_CLOSE_LIQUIDATION_KIS_HOLDING",
                source="kis_holdings",
            )
        elif kis_client is not None and hasattr(kis_client, "sell_stock_market"):
            resp = kis_client.sell_stock_market(code, qty)
        else:
            raise RuntimeError("KIS_SELL_SUBMIT_UNAVAILABLE")
        ok = _is_accepted_order_response(resp)
        rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
        msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
        msg1 = resp.get("msg1") if isinstance(resp, dict) else None
        logger.info("[KIS][ORDER][RESPONSE] side=SELL code=%s rt_cd=%s msg_cd=%s msg1=%s", code, rt_cd, msg_cd, msg1)
        logger.info("[TRADE][ORDER][SELL] code=%s result=%s", code, "ACCEPTED" if ok else "REJECTED")
        result = dict(resp) if isinstance(resp, dict) else {"resp": resp}
        result.update({"side": "SELL", "code": code, "qty": qty, "reason": "KR_CLOSE_LIQUIDATION_KIS_HOLDING", "source": "kis_holdings", "accepted": ok, "result": "ACCEPTED" if ok else "REJECTED"})
        results.append(result)
    if not results:
        logger.warning("[KR_CLOSE][LIQUIDATION][NO_ORDER_CREATED] source=kis_holdings holdings=%s", len(rows))
    return results


def run_emergency_close_liquidation_from_kis_holdings(**kwargs: Any) -> list[dict[str, Any]]:
    return run_close_liquidation_from_kis_holdings(**kwargs)


def build_close_liquidation_orders_from_kis_holdings(kis_holdings: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Build dry-run close liquidation intents without emitting fake API success logs."""
    return run_emergency_close_liquidation_from_kis_holdings(kis_client=None, holdings=kis_holdings, dry_run=True)

def _is_close_or_exit_only_session(*, phase_name: str | None, session_kind: str | None) -> bool:
    return (
        str(phase_name or "").strip().lower() in {"exit", "close", "pm_exit_only"}
        or str(session_kind or "").strip().lower() in {"close", "exit", "trade-close"}
        or str(os.getenv("PB1_ENTRY_ENABLED", "1")).strip() == "0"
        or str(os.getenv("ENTRY_ENABLED", "1")).strip() == "0"
    )

def _guard_empty_final30_for_engine_boot(
    precomputed_final30_df: pd.DataFrame | None,
    *,
    phase_name: str,
    session_kind: str,
) -> pd.DataFrame:
    if precomputed_final30_df is not None and not precomputed_final30_df.empty:
        return precomputed_final30_df
    if _is_close_or_exit_only_session(phase_name=phase_name, session_kind=session_kind):
        logger.warning(
            "[TRADE][ENGINE_BOOT][FINAL30_BYPASS_FOR_CLOSE_EXIT] phase=%s session_kind=%s reason=allow_kis_holdings_liquidation",
            phase_name,
            session_kind,
        )
        return pd.DataFrame()
    logger.error(
        "[TRADE][ENGINE_BOOT][BLOCKED] reason=empty_final30_after_all_fallbacks phase=%s session_kind=%s",
        phase_name,
        session_kind,
    )
    raise RuntimeError("TRADE_FINAL30_EMPTY_AFTER_ALL_FALLBACKS")

def load_trade_final30_scored(
    *,
    engine,
    env: str,
    as_of: str,
) -> dict[str, Any]:
    env_n = (env or "").strip().lower()
    as_of_s = str(as_of)
    logger.info("[TRADE][FINAL30][LOAD_START] preferred_source=db")
    repo_root_path = resolve_repo_root()
    cache_key = (str(repo_root_path), env_n, as_of_s)
    cached = _FINAL30_LOAD_CACHE.get(cache_key)
    if cached is not None:
        cached_df = cached.get("df")
        logger.info(
            "[TRADE][FINAL30][CACHE_HIT] env=%s as_of=%s source=%s rows=%s",
            env_n,
            as_of_s,
            cached.get("source_name"),
            len(cached_df) if isinstance(cached_df, pd.DataFrame) else 0,
        )
        result = dict(cached)
        if isinstance(cached_df, pd.DataFrame):
            result["df"] = cached_df.copy(deep=True)
        return result
    final30_paths = build_final30_paths(repo_root_path, env_n, as_of_s)
    logger.info(
        "[TRADE][FINAL30][PATH_MAP] repo_root=%s cwd=%s paths=%s",
        repo_root_path,
        Path.cwd(),
        serialize_path_map(final30_paths),
    )
    logger.info(
        "[FINAL30][PATH_MAP] runtime=%s ledger=%s signals=%s db=%s",
        final30_paths.get("runtime"),
        final30_paths.get("ledger"),
        final30_paths.get("signals"),
        "pb1_watchlist_final_scored",
    )
    file_mirror_stats: dict[str, int] = {}
    for label, path in final30_paths.items():
        present = int(path.exists() and path.stat().st_size > 0) if path.exists() else 0
        file_mirror_stats[label] = present
        if not present:
            logger.warning("[TRADE][FINAL30][FILE_OPTIONAL_MISSING] path=%s", path.resolve())

    logger.info(
        "[TRADE][FINAL30][FILE_MIRROR] runtime=%s ledger=%s signals=%s warn_only=1",
        file_mirror_stats.get("runtime", 0),
        file_mirror_stats.get("ledger", 0),
        file_mirror_stats.get("signals", 0),
    )

    watchlist_repo = WatchlistRepo(engine)
    as_of_date = datetime.strptime(as_of_s, "%Y-%m-%d").date()
    scored_contract = watchlist_repo.verify_watchlist_scored_contract(
        env=env_n,
        as_of=as_of_date,
        strategy="pb1_watchlist_final_scored",
        allow_latest_fallback=False,
    )
    rows = list(scored_contract.get("rows_data") or [])
    contract_columns = [str(col) for col in (scored_contract.get("columns") or sorted({key for row in rows for key in (row or {}).keys()}))]
    missing_critical_fields = _missing_scored_cols(contract_columns)
    flow_optional_missing = _safe_flow_optional_missing(contract_columns)
    first_row_keys = sorted(rows[0].keys()) if rows else []

    source_compare: dict[str, list[str]] = {"db": list(missing_critical_fields)}
    for label, path in final30_paths.items():
        file_rows, json_ok = read_final30_file_rows(path)
        if json_ok and file_rows:
            file_cols = sorted({str(key) for row in file_rows for key in (row or {}).keys()})
            source_compare[label] = _missing_scored_cols(file_cols)
        else:
            source_compare[label] = list(REQUIRED_FINAL30_SCORED_COLS)

    repairable_contract = _is_trade_repairable_db_contract(scored_contract, missing_critical_fields=missing_critical_fields)
    contract_ok = bool(
        (
            (
                scored_contract.get("ok")
                and int(scored_contract.get("rows") or 0) == 30
                and int(scored_contract.get("uniq_codes") or 0) == 30
            )
            or repairable_contract
        )
        and not missing_critical_fields
    )
    logger.info(
        "[TRADE][FINAL30][DB_EXACT_LOAD] env=%s as_of=%s rows=%s uniq_codes=%s uniq_ranks=%s",
        env_n,
        as_of_date.isoformat(),
        scored_contract.get("rows"),
        scored_contract.get("uniq_codes"),
        scored_contract.get("uniq_ranks"),
    )
    logger.info(
        "[TRADE][FINAL30][CONTRACT_CHECK][PRE_REPAIR] ok=%s repairable=%s rows=%s null_critical=%s missing_fields=%s missing_critical=%s",
        int(contract_ok),
        int(repairable_contract),
        scored_contract.get("rows"),
        scored_contract.get("null_critical"),
        scored_contract.get("missing_fields"),
        missing_critical_fields,
    )
    logger.info(
        "[FINAL30][SOURCE_CHECK] db_rows=%s file_runtime_exists=%s file_ledger_exists=%s file_signals_exists=%s usable=%s required_scored_cols_missing=%s flow_optional_missing=%s",
        int(scored_contract.get("rows") or 0),
        file_mirror_stats.get("runtime", 0),
        file_mirror_stats.get("ledger", 0),
        file_mirror_stats.get("signals", 0),
        int(contract_ok),
        missing_critical_fields,
        flow_optional_missing,
    )
    logger.info(
        "[TRADE][FINAL30][STRICT_VALIDATE][DB] ok=%s rows=%s invalid_rows=%s errors=%s warnings=%s",
        int(bool(scored_contract.get("ok"))),
        int(scored_contract.get("rows") or 0),
        int(scored_contract.get("invalid_row_count") or 0),
        list(scored_contract.get("errors") or []),
        list(scored_contract.get("warnings") or []),
    )
    db_distribution = summarize_entry_style_distribution(rows)
    logger.info(
        "[TRADE][FINAL30][ENTRY_STYLE_DISTRIBUTION] total=%s counts=%s",
        int(db_distribution.get("total") or 0),
        dict(db_distribution.get("counts") or {}),
    )
    logger.info(
        "[FINAL30][SOURCE_SUMMARY] source=db_pb1_watchlist_final_scored rows=%s as_of=%s locked=%s usable=%s",
        len(rows),
        as_of_s,
        int(contract_ok),
        int(contract_ok),
    )
    logger.info("[FINAL30][SOURCE_COLS] source=db_pb1_watchlist_final_scored cols=%s", contract_columns)
    logger.info("[FINAL30][SOURCE_SAMPLE_KEYS] source=db_pb1_watchlist_final_scored first_row_keys=%s", first_row_keys)
    logger.info("[FINAL30][REQUIRED_CHECK] source=db_pb1_watchlist_final_scored missing=%s", missing_critical_fields)
    logger.info("[FINAL30][FLOW_OPTIONAL_CHECK] source=db_pb1_watchlist_final_scored missing=%s", flow_optional_missing)
    logger.info(
        "[FINAL30][SOURCE_COMPARE] db_missing=%s runtime_missing=%s ledger_missing=%s signals_missing=%s",
        source_compare.get("db", []),
        source_compare.get("runtime", []),
        source_compare.get("ledger", []),
        source_compare.get("signals", []),
    )
    if contract_ok:
        missing_labels = [label for label, present in file_mirror_stats.items() if not present]
        if missing_labels and rows:
            logger.info(
                "[TRADE][FINAL30][FILE_REPAIR][START] env=%s as_of=%s missing=%s source=db_pb1_watchlist_final_scored",
                env_n,
                as_of_s,
                missing_labels,
            )
            repair_results = write_final30_mirrors(
                repo_root=repo_root_path,
                env=env_n,
                as_of=as_of_s,
                rows=rows,
                source="trade_db_final30_scored_repair",
            )
            file_mirror_stats = {
                label: int(bool((repair_results.get(label) or {}).get("ok")))
                for label in final30_paths.keys()
            }
            logger.info(
                "[TRADE][FINAL30][FILE_REPAIR][DONE] runtime=%s ledger=%s signals=%s",
                file_mirror_stats.get("runtime", 0),
                file_mirror_stats.get("ledger", 0),
                file_mirror_stats.get("signals", 0),
            )
        _validate_trade_final30_files(repo_root_path=repo_root_path, env=env_n, as_of=as_of_s)
        if missing_labels and not any(file_mirror_stats.values()):
            logger.error(
                "[TRADE][FINAL30][FILE_CONTRACT_MISMATCH] env=%s as_of=%s paths=%s",
                env_n,
                as_of_s,
                serialize_path_map(final30_paths),
            )
        working_rows = [normalize_final30_contract_row(dict(row or {})) for row in rows]
        if "ma20_invalid_rows" in set(scored_contract.get("errors") or []):
            logger.info("[TRADE][FINAL30][REPAIR][START] rows=%s source=db_exact", len(working_rows))
            working_rows, repair_stats = _repair_trade_final30_rows(
                engine=engine,
                env=env_n,
                as_of=as_of_s,
                rows=working_rows,
                repo_root_path=repo_root_path,
            )
            logger.info(
                "[TRADE][FINAL30][REPAIR][MA20] repaired=%s unresolved=%s",
                repair_stats.get("repaired", 0),
                repair_stats.get("unresolved", 0),
            )
            filtered_rows = [
                row for row in working_rows
                if safe_nullable_float((row or {}).get("ma20")) is not None and float((row or {}).get("ma20") or 0.0) > 0
            ]
            logger.info(
                "[TRADE][FINAL30][FILTER_AFTER_REPAIR] before=%s after=%s dropped_ma20=%s",
                len(working_rows),
                len(filtered_rows),
                len(working_rows) - len(filtered_rows),
            )
            working_rows = filtered_rows
        trade_validate = _trade_recheck_rows(working_rows, source="db_recheck") if working_rows else {
            "ok": False,
            "rows": 0,
            "errors": ["rows_not_exact"],
            "invalid_row_count": 0,
            "warnings": [],
            "invalid_details": {},
            "invalid_sample_codes": [],
            "source": "db_recheck",
        }
        min_tradeable_candidates = _trade_min_tradeable_candidates()
        trade_recheck_ok = bool(trade_validate.get("ok")) and len(working_rows) >= min_tradeable_candidates
        logger.info(
            "[TRADE][FINAL30][RECHECK] ok=%s rows=%s min_tradeable=%s errors=%s",
            int(trade_recheck_ok),
            len(working_rows),
            min_tradeable_candidates,
            list(trade_validate.get("errors") or []),
        )
        logger.info(
            "[TRADE][FINAL30][CONTRACT_CHECK] ok=%s rows=%s null_critical=%s missing_fields=%s missing_critical=%s",
            int(trade_recheck_ok),
            len(working_rows),
            scored_contract.get("null_critical"),
            scored_contract.get("missing_fields"),
            missing_critical_fields,
        )
        if missing_labels:
            _selected_source, working_rows = _select_repaired_final30_source(
                repo_root_path=repo_root_path,
                env=env_n,
                as_of=as_of_s,
                db_rows=working_rows,
            )
        df = pd.DataFrame(working_rows)
        columns = [str(c) for c in df.columns.tolist()]
        try:
            if not trade_recheck_ok:
                raise RuntimeError(format_final30_abort_message(trade_validate))
            logger.info(
                "[FINAL30][INPUT_CHECK] source=db_pb1_watchlist_final_scored usable=%s required_scored_cols_missing=%s",
                int(trade_recheck_ok),
                missing_critical_fields,
            )
            logger.info(
                "[FINAL30][SOURCE_SUMMARY] source=db_pb1_watchlist_final_scored rows=%s as_of=%s locked=1 usable=%s",
                len(df),
                as_of_s,
                int(trade_recheck_ok),
            )
            logger.info("[TRADE][FINAL30][LOAD_RESULT] source=db_pb1_watchlist_final_scored rows=%s usable=%s", len(df), int(trade_recheck_ok))
            logger.info("[TRADE][FINAL30][FLOW_OPTIONAL] missing=%s", flow_optional_missing)
            logger.info("[TRADE][READY][OK] source=%s as_of=%s rows=%s", "db_pb1_watchlist_final_scored", as_of_date.isoformat(), len(df))
        except RuntimeError as exc:
            failure_reason = _classify_final30_abort_reason(
                rows=len(working_rows),
                missing_cols=missing_critical_fields,
                errors=list(trade_validate.get("errors") or []),
            )
            logger.info("[FINAL30][INPUT_CHECK] source=db_pb1_watchlist_final_scored usable=0 required_scored_cols_missing=%s", missing_critical_fields)
            logger.info(
                "[FINAL30][SOURCE_SUMMARY] source=db_pb1_watchlist_final_scored rows=%s as_of=%s locked=0 usable=0",
                len(working_rows),
                as_of_s,
            )
            logger.info("[TRADE][FINAL30][LOAD_RESULT] source=db_pb1_watchlist_final_scored rows=%s usable=0", len(working_rows))
            logger.info("[TRADE][FINAL30][REJECT_REASON] missing_scored_cols=%s", missing_critical_fields)
            logger.info("[TRADE][FINAL30][FLOW_OPTIONAL] missing=%s", flow_optional_missing)
            logger.error("[TRADE][PRECHECK][FINAL30] status=FAIL reason=db_contract_invalid")
            logger.error("[FINAL30][ABORT] reason=%s", failure_reason)
            logger.error("[TRADE][FINAL30][FAIL] reason=%s", failure_reason)
            logger.error("[TRADE][READY][FAIL] reason=final30_contract_invalid")
            logger.error("[TRADE][ABORT][FINAL30_INVALID][DETAIL] %s", format_final30_abort_message(trade_validate))
            logger.error("%s", exc)
            result = _empty_final30_load_result(
                final30_paths=final30_paths,
                missing_scored_cols=list(missing_critical_fields),
                reason=failure_reason,
            )
            result["as_of"] = as_of_s
            result["file_mirror_present"] = any(file_mirror_stats.values())
            result["flow_optional_missing"] = list(flow_optional_missing)
            result["source_compare"] = source_compare
            _FINAL30_LOAD_CACHE[cache_key] = {**result, "df": pd.DataFrame()}
            return result
        logger.info(
            "[TRADE][FINAL30][LOAD] source=db_pb1_watchlist_final_scored rows=%s is_scored=1 cols=%s",
            len(df),
            columns,
        )
        logger.info(
            "[TRADE][FINAL30][LOCK][OK] source=%s as_of=%s rows=%s",
            "db_pb1_watchlist_final_scored",
            as_of_date.isoformat(),
            len(df),
        )
        result = {
            "df": df,
            "source_name": "db_pb1_watchlist_final_scored",
            "as_of": as_of_date.isoformat(),
            "columns": columns,
            "is_scored": True,
            "used_fallback": False,
            "file_mirror_present": any(file_mirror_stats.values()),
            "usable": bool(trade_recheck_ok),
            "missing_scored_cols": list(missing_critical_fields),
            "flow_optional_missing": list(flow_optional_missing),
            "path_map": {key: str(value) for key, value in final30_paths.items()},
            "source_compare": source_compare,
        }
        _FINAL30_LOAD_CACHE[cache_key] = {**result, "df": df.copy(deep=True)}
        return result

    failure_reason = _classify_final30_abort_reason(
        rows=len(rows),
        missing_cols=missing_critical_fields,
        errors=list(scored_contract.get("errors") or []),
    )
    logger.info("[FINAL30][INPUT_CHECK] source=db_pb1_watchlist_final_scored usable=0 required_scored_cols_missing=%s", missing_critical_fields)
    logger.info(
        "[FINAL30][SOURCE_SUMMARY] source=db_pb1_watchlist_final_scored rows=%s as_of=%s locked=0 usable=0",
        len(rows),
        as_of_s,
    )
    logger.info("[TRADE][FINAL30][LOAD_RESULT] source=db_pb1_watchlist_final_scored rows=%s usable=0", len(rows))
    logger.info("[TRADE][FINAL30][REJECT_REASON] missing_scored_cols=%s", missing_critical_fields)
    logger.info("[TRADE][FINAL30][FLOW_OPTIONAL] missing=%s", flow_optional_missing)
    logger.error("[FINAL30][ABORT] reason=%s", failure_reason)
    logger.error(
        "[TRADE][ABORT][FINAL30_INVALID][DETAIL] source=db rows=%s invalid_rows=%s error_codes=%s details=%s",
        scored_contract.get("rows"),
        scored_contract.get("invalid_row_count"),
        scored_contract.get("invalid_sample_codes"),
        scored_contract.get("invalid_details"),
    )
    logger.error(
        "[TRADE][FINAL30][LOAD_FAIL] reason=db_contract_invalid env=%s as_of=%s rows=%s uniq_codes=%s uniq_ranks=%s null_critical=%s missing_fields=%s missing_critical=%s errors=%s",
        env_n,
        as_of_date.isoformat(),
        scored_contract.get("rows"),
        scored_contract.get("uniq_codes"),
        scored_contract.get("uniq_ranks"),
        scored_contract.get("null_critical"),
        scored_contract.get("missing_fields"),
        missing_critical_fields,
        scored_contract.get("errors"),
    )
    result = _empty_final30_load_result(
        final30_paths=final30_paths,
        missing_scored_cols=list(missing_critical_fields),
        reason=failure_reason,
    )
    result["as_of"] = as_of_s
    result["file_mirror_present"] = any(file_mirror_stats.values())
    result["flow_optional_missing"] = list(flow_optional_missing)
    result["source_compare"] = source_compare
    _FINAL30_LOAD_CACHE[cache_key] = {**result, "df": pd.DataFrame()}
    return result


def _is_nontrading_eval_mode(
    *,
    trading_day: bool,
    strategy_mode: str,
    force_block_live: bool,
    order_allowed: bool,
) -> bool:
    if _env_bool_any(("NONTRADING_EVAL_MODE",), default=False):
        return True
    return (not trading_day) or strategy_mode.upper() == "DIAG" or bool(force_block_live) or not bool(order_allowed)


def _write_nontrading_eval_reports(
    *,
    engine_runner: object,
    result_status: str,
) -> None:
    entry_path = runtime_path("runtime", "reports", "nontrading_entry_eval.json")
    exit_path = runtime_path("runtime", "reports", "nontrading_exit_eval.json")
    entry_path.parent.mkdir(parents=True, exist_ok=True)
    entry_payload = {
        "status": result_status,
        "scanner": getattr(engine_runner, "_scanner_summary", {}) or {},
        "entry_summary": getattr(engine_runner, "_run_summary_payload", {}) or {},
        "entry_debug": getattr(engine_runner, "_debug_summary", {}) or {},
        "order_candidates": list((getattr(engine_runner, "_debug_summary", {}) or {}).get("order_candidate_codes", []) or []),
    }
    exit_payload = {
        "status": result_status,
        "holdings_meta": getattr(engine_runner, "_exit_holdings_meta", {}) or {},
        "exit_summary": getattr(engine_runner, "_exit_summary_payload", {}) or {},
        "exit_evaluations": getattr(engine_runner, "_exit_evaluations", []) or [],
    }
    entry_path.write_text(json.dumps(entry_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    exit_path.write_text(json.dumps(exit_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[NONTRADING_EVAL][REPORT] entry=%s exit=%s", entry_path, exit_path)


def check_prep_done() -> bool:
    """
    Check if PREP has completed and final30 watchlist is available.
    
    Returns:
        bool: True if PREP is ready, False otherwise
    """
    try:
        env_name = os.getenv("KIS_ENV", "practice")
        as_of = now_kst().date().isoformat()
        repo_root_path = resolve_repo_root()
        final30_paths = build_final30_paths(repo_root_path, env_name, as_of)
        snapshot_path = build_watchlist_paths(repo_root_path, as_of)["snapshot"]
        final30_candidates = [snapshot_path, *final30_paths.values()]
        logger.info("[PREP_CHECK][FINAL30][PATHS] %s", {key: str(path) for key, path in final30_paths.items()})

        for candidate_path in final30_candidates:
            if candidate_path.exists() and candidate_path.stat().st_size > 100:
                logger.info("[PREP_CHECK] Found final30 at %s", candidate_path)
                return True
        
        logger.warning("[PREP_CHECK] PREP outputs not found")
        return False
    except Exception as e:
        logger.error("[PREP_CHECK] Error checking PREP status: %s", e)
        return False


def _resolve_trade_prep_done_status(
    *,
    ledger_repo: LedgerEventsRepo,
    watchlist_repo: WatchlistRepo,
    env: str,
    strategy: str,
    as_of: date,
    trade_date: date,
) -> tuple[bool, str, int, dict[str, Any]]:
    try:
        prep_done, prep_done_count = ledger_repo.prep_done_status(
            env=env,
            as_of=as_of,
            strategies=[strategy],
            trade_date=trade_date,
        )
    except TypeError:
        prep_done, prep_done_count = ledger_repo.prep_done_status(
            env=env,
            as_of=as_of,
        )
    if prep_done:
        logger.info(
            "[TRADE_TICK][PREP_DONE_CHECK] source=db_ledger prep_done=1 matched_events_count=%s",
            prep_done_count,
        )
        return True, "db_ledger", prep_done_count, {
            "final30_count": 0,
            "contract_ok": True,
            "usable": True,
            "missing_critical_fields": [],
        }

    try:
        canonical = watchlist_repo.verify_watchlist_scored_contract(
            env=env,
            as_of=as_of,
            strategy="pb1_watchlist_final_scored",
            allow_latest_fallback=False,
            log_result=False,
        )
    except Exception:
        canonical = {
            "rows": 0,
            "ok": False,
            "columns": [],
            "env": env,
            "strategy": "pb1_watchlist_final_scored",
            "as_of": as_of.isoformat(),
        }
    canonical_columns = [str(item) for item in (canonical.get("columns") or [])]
    missing_critical_fields = [
        col for col in REQUIRED_FINAL30_SCORED_COLS
        if col not in canonical_columns
    ]
    final30_count = int(canonical.get("rows") or 0)
    contract_ok = bool(canonical.get("ok"))
    usable = bool(final30_count == 30 and contract_ok and not missing_critical_fields)
    env_match = str(canonical.get("env") or "").strip().lower() == str(env).strip().lower()
    strategy_match = str(canonical.get("strategy") or "").strip().lower() == "pb1_watchlist_final_scored"
    as_of_match = str(canonical.get("as_of") or "") == as_of.isoformat()
    fallback_ok = bool(
        final30_count == 30
        and contract_ok
        and usable
        and not missing_critical_fields
        and env_match
        and strategy_match
        and as_of_match
    )

    if fallback_ok:
        logger.warning(
            "[TRADE_TICK][PREP_DONE_CHECK][FALLBACK_CANONICAL_OK] ledger_prep_done=0 fallback_prep_done=1 final30_count=%s",
            final30_count,
        )
        ledger_repo.upsert_prep_event(
            env=env,
            strategy=strategy,
            as_of=as_of,
            trade_date=trade_date,
            event_type="PREP_DONE",
            status="READY_FROM_FINAL30_FALLBACK",
            reason="ledger_missing_but_final30_canonical_ok",
            final30_count=final30_count,
            quality_ok=True,
            trade_can_proceed=True,
        )
        logger.info("[TRADE_TICK][PREP_DONE_CHECK] source=final30_canonical_fallback prep_done=1")
        return True, "final30_canonical_fallback", 0, {
            "final30_count": final30_count,
            "contract_ok": contract_ok,
            "usable": usable,
            "missing_critical_fields": missing_critical_fields,
        }

    logger.warning(
        "[TRADE_TICK][PREP_DONE_CHECK][FAIL] reason=PREP_NOT_DONE ledger_prep_done=0 final30_count=%s contract_ok=%s usable=%s missing=%s",
        final30_count,
        int(contract_ok),
        int(usable),
        missing_critical_fields,
    )
    return False, "none", 0, {
        "final30_count": final30_count,
        "contract_ok": contract_ok,
        "usable": usable,
        "missing_critical_fields": missing_critical_fields,
    }


def load_snapshot_fallback(snapshot_name: str = "final30") -> list | None:
    """
    Load snapshot from JSON file as fallback when PREP missing.
    
    Args:
        snapshot_name: snapshot file name (e.g., "final30")
    
    Returns:
        List of watchlist items or None on failure
    """
    try:
        runtime_base = Path(os.getenv("GITHUB_WORKSPACE", "."))
        runtime_dir = runtime_base / "repo" / "runtime" if (runtime_base / "repo").exists() else runtime_base / "runtime"
        
        snapshot_path = runtime_dir / "snapshots" / f"{snapshot_name}.json"
        
        if not snapshot_path.exists():
            logger.warning("[SNAPSHOT_FALLBACK] File not found: %s", snapshot_path)
            return None
        
        with snapshot_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        watchlist = data.get("watchlist", [])
        
        if not watchlist:
            logger.warning("[SNAPSHOT_FALLBACK] Empty watchlist in snapshot")
            return None
        
        logger.info("[SNAPSHOT_FALLBACK] Loaded %s items from %s", len(watchlist), snapshot_path)
        return watchlist
        
    except Exception as e:
        logger.error("[SNAPSHOT_FALLBACK] Error loading snapshot: %s", e)
        return None


def resolve_env(cli_env: str | None) -> str:
    if cli_env:
        return str(cli_env).strip().lower()
    strategy_env = os.getenv("STRATEGY_ENV")
    if strategy_env:
        return str(strategy_env).strip().lower()
    kis_env = os.getenv("KIS_ENV")
    if kis_env:
        return str(kis_env).strip().lower()
    return "practice"


def _fail_open_on_runs_ledger_error(env: str | None) -> bool:
    env_name = (str(env or os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice").strip().lower() or "practice")
    raw_flag = os.getenv("PB1_FAIL_OPEN_ON_RUNS_LEDGER_ERROR")
    if raw_flag is None:
        return env_name == "practice"
    flag_enabled = str(raw_flag).strip().lower() not in {"0", "false", "no", "off"}
    if env_name == "practice":
        return flag_enabled
    return flag_enabled


def _parse_as_of_override(value: str) -> datetime.date:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("empty AS_OF value")
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"invalid AS_OF format: {raw}") from exc


def _force_live_env_lock_if_needed(intended_live: bool) -> bool:
    """
    If we intend to trade live, we must guarantee env flags are consistent.
    This prevents any later re-reads from flipping DRY_RUN back to '1'.
    
    CRITICAL: Once intended_live=True, environment variables must be locked
    to prevent any code path from re-reading or re-setting them to safe defaults.
    
    Returns:
        bool: parsed dry_run value after lock (must be False for intended_live=True)
    """
    if not intended_live:
        # Safe mode: just parse and return using safe parser
        dry_run = _env_bool_any(("DRY_RUN", "DRYRUN"), default=True)
        return dry_run

    # Hard lock: once live-intended, env must not block orders.
    os.environ["DRY_RUN"] = "0"
    os.environ["DISABLE_LIVE_TRADING"] = "0"
    os.environ["LIVE_TRADING_ENABLED"] = "1"
    os.environ["SIMULATION_MODE"] = "0"

    logger.info(
        "[LIVE_ENV_LOCK] 🔒 FORCE LOCK: DRY_RUN=%s DISABLE_LIVE_TRADING=%s LIVE_TRADING_ENABLED=%s SIMULATION_MODE=%s",
        os.getenv("DRY_RUN"),
        os.getenv("DISABLE_LIVE_TRADING"),
        os.getenv("LIVE_TRADING_ENABLED"),
        os.getenv("SIMULATION_MODE"),
    )
    
    # ✅ CRITICAL: Parse dry_run AFTER lock using safe parser (must be single source of truth)
    dry_run = parse_bool_any(os.getenv("DRY_RUN"), default=True)
    
    # ✅ FATAL: If env=0 but parsed=True, something is broken
    if os.getenv("DRY_RUN") == "0" and dry_run is True:
        raise RuntimeError(
            f"BUG: DRY_RUN=0 parsed as True. "
            f"parse_bool_any import/implementation is broken. "
            f"env={os.getenv('DRY_RUN')} parsed={dry_run} type={type(dry_run).__name__}"
        )
    
    logger.info(
        "[LIVE_ENV_LOCK][DRY_RUN] env=%s parsed=%s (type=%s) intended_live=%s",
        os.getenv("DRY_RUN"),
        dry_run,
        type(dry_run).__name__,
        intended_live,
    )
    
    return dry_run


def resolve_auto_strategy_mode(mode_env: str) -> str:
    """
    AUTO 모드 결정: 시간 기반으로 LIVE/DIAG 결정.
    
    Args:
        mode_env: STRATEGY_MODE 환경변수 값
    
    Returns:
        "LIVE" or "DIAG"
    
    정책:
        - mode_env가 "LIVE" 또는 "DIAG"이면 그대로 사용
        - mode_env가 "AUTO"이면:
            - 장중(09:00~15:20, 월~금): "LIVE"
            - 장외(주말, 장시작 전, 장마감 후): "DIAG"
    """
    mode_env = (mode_env or "").strip().upper()
    if mode_env in ("LIVE", "DIAG"):
        logger.info("[AUTO_MODE] mode=%s (forced)", mode_env)
        return mode_env
    
    # AUTO 모드: 시간 기반 결정
    is_market_open = is_market_open_kst(now_kst())
    resolved_mode = "LIVE" if is_market_open else "DIAG"
    logger.info(
        "[AUTO_MODE] mode=AUTO resolved=%s is_market_open=%s now=%s",
        resolved_mode,
        is_market_open,
        now_kst().strftime("%Y-%m-%d %H:%M:%S"),
    )
    return resolved_mode


def compute_loop_deadline(now: datetime) -> datetime:
    """
    루프 종료 시각 계산: 장 마감 - grace_min.
    
    Args:
        now: 현재 KST 시각
    
    Returns:
        루프 종료 시각 (KST)
    """
    grace_min = int(os.getenv("PB1_LOOP_GRACE_MIN", "3"))
    market_close = market_close_dt_kst(now)
    deadline = market_close - timedelta(minutes=grace_min)
    logger.info(
        "[LOOP_DEADLINE] market_close=%s grace_min=%s deadline=%s",
        market_close.strftime("%H:%M:%S"),
        grace_min,
        deadline.strftime("%H:%M:%S"),
    )
    return deadline


def normalize_session_kind(raw: str) -> str:
    """Normalize session kind but NEVER convert close to afternoon.
    
    close must remain close for proper session_end calculation.
    """
    raw = (raw or "").strip().lower()
    # CRITICAL: close must never be converted to afternoon
    if raw == "close" or raw == "trade-close":
        return "close"
    # Only pm (not close) converts to afternoon
    if raw in {"pm", "trade-pm"}:
        return "afternoon"
    if raw in {"am", "morning"}:
        return "am"
    if raw in {"afternoon", "trade-afternoon"}:
        return "afternoon"
    return raw


def _resolve_trade_session(now: datetime) -> str:
    forced_session = str(os.getenv("PB1_FORCE_TRADE_SESSION") or "auto").strip().lower() or "auto"
    if forced_session in {"am", "pm"}:
        return forced_session
    explicit_session = str(os.getenv("PB1_SESSION_KIND") or "").strip().lower()
    if explicit_session in {"am", "pm"}:
        return explicit_session
    now_minutes = now.hour * 60 + now.minute
    if 9 * 60 <= now_minutes < 12 * 60 + 59:
        return "am"
    if 13 * 60 <= now_minutes < 15 * 60 + 39:
        return "pm"
    return "am" if now_minutes < 13 * 60 else "pm"


def _resolve_session_kind(now: datetime | None = None) -> str:
    raw_value = str(os.getenv("PB1_SESSION_KIND") or "").strip().lower()
    raw_window = str(os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
    raw_force_session = str(os.getenv("PB1_FORCE_TRADE_SESSION") or "").strip().lower()
    
    # CRITICAL: close must never be converted to afternoon
    # Check explicit settings first
    if raw_value == "close" or raw_window == "close" or raw_force_session == "close":
        logger.info(
            "[PB1][SESSION_NORMALIZE] raw_session=%s normalized_session=close raw_window=%s raw_force=%s",
            raw_value,
            raw_window,
            raw_force_session,
        )
        return "close"
    
    normalized = normalize_session_kind(raw_value)
    normalized_window = normalize_session_kind(raw_window) if raw_window else raw_window
    if normalized != raw_value or normalized_window != raw_window:
        logger.info(
            "[PB1][SESSION_NORMALIZE] raw_session=%s normalized_session=%s raw_window=%s normalized_window=%s",
            raw_value,
            normalized,
            raw_window,
            normalized_window,
        )
    
    if raw_value in {"morning", "am"}:
        return "am"
    if raw_value in {"pm", "afternoon"}:
        return "afternoon"
    if normalized in {"am", "afternoon", "close"}:
        return normalized
    return _resolve_trade_session(now or _get_now_kst())


def _resolve_session_end_dt(now: datetime) -> datetime:
    session_kind = _resolve_session_kind()
    am_end = os.getenv("PB1_AM_SESSION_END", "13:00")
    pm_end = os.getenv("PB1_PM_SESSION_END", "15:10")
    close_end = os.getenv("PB1_CLOSE_SESSION_END", CLOSE_AUCTION_END or MARKET_CLOSE_HHMM or "15:30")

    if session_kind == "am":
        end_hhmm = am_end
    elif session_kind == "close":
        end_hhmm = close_end
    else:
        end_hhmm = pm_end

    try:
        end_time = _parse_hhmm_to_time(end_hhmm)
    except Exception:
        fallback_hhmm = "15:30" if session_kind == "close" else ("13:00" if session_kind == "am" else "15:10")
        logger.warning(
            "[PB1][LOOP][SESSION] invalid session end override kind=%s raw=%s fallback=%s",
            session_kind,
            end_hhmm,
            fallback_hhmm,
        )
        end_time = _parse_hhmm_to_time(fallback_hhmm)

    return now.replace(
        hour=end_time.hour,
        minute=end_time.minute,
        second=0,
        microsecond=0,
    )


def _session_guard_spec(session_kind: str) -> dict[str, str] | None:
    normalized = str(session_kind or "").strip().lower()
    specs = {
        "am": {
            "strategy": "pb1_am_session_guard",
            "run_window": "morning",
            "phase": "entry",
            "log_prefix": "TRADE_AM",
            "expected_start_window": "0907-0915",
        },
        "pm": {
            "strategy": "pb1_pm_session_guard",
            "run_window": "day",
            "phase": "entry",
            "log_prefix": "TRADE_PM",
            "expected_start_window": "1305-1315",
        },
        "close": {
            "strategy": "pb1_close_session_guard",
            "run_window": "close",
            "phase": "exit",
            "log_prefix": "TRADE_CLOSE",
            "expected_start_window": "1515-1524",
        },
    }
    return specs.get(normalized)


def _normalize_hhmm_compact(raw: str | None, fallback: str) -> str:
    candidate = str(raw or fallback).strip().replace(":", "")
    if len(candidate) == 4 and candidate.isdigit():
        return candidate
    fallback_compact = str(fallback).strip().replace(":", "")
    logger.warning(
        "[PB1][RECOVERY][ENV] invalid_hhmm raw=%s fallback=%s",
        raw,
        fallback_compact,
    )
    return fallback_compact


def _parse_hhmm_minutes(raw: str | None, fallback: str) -> int:
    compact = _normalize_hhmm_compact(raw, fallback)
    return int(compact[:2]) * 60 + int(compact[2:])


def _parse_hhmm_kst(value: str | None, now: datetime, fallback: str = "0000") -> datetime:
    compact = _normalize_hhmm_compact(value, fallback)
    return now.replace(
        hour=int(compact[:2]),
        minute=int(compact[2:]),
        second=0,
        microsecond=0,
    )


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return parse_bool_any(raw, default=default)


def _session_cfg(session: str) -> dict[str, Any] | None:
    normalized = str(session or "").strip().lower()
    configs: dict[str, dict[str, Any]] = {
        "am": {
            "session": "am",
            "log_prefix": "TRADE_AM",
            "session_start": "0900",
            "session_end_exclusive": "1259",
            "valid_window": "0900-1255",
            "valid_until": "1255",
            "expected_from_env": "PB1_AM_EXPECTED_START",
            "expected_from_legacy_env": "PB1_AM_EXPECTED_START_FROM",
            "expected_from_default": "0907",
            "expected_to_env": "PB1_AM_EXPECTED_END",
            "expected_to_legacy_env": "PB1_AM_EXPECTED_START_TO",
            "expected_to_default": "0915",
            "start_allow_env": "PB1_AM_START_ALLOW_UNTIL",
            "start_allow_default": "0930",
            "recovery_allow_env": "PB1_AM_RECOVERY_ALLOW_UNTIL",
            "recovery_allow_default": "1030",
            "enable_env": "PB1_ENABLE_LATE_AM_RECOVERY",
            "force_late_env": "PB1_FORCE_TRADE_AM_LATE_START",
            "force_hard_env": "PB1_FORCE_TRADE_AM_AFTER_1030",
            "manual_hard_cutoff_reason": "late_manual_start_after_1030_requires_force",
            "schedule_allow_window_reason": "late_schedule_start_after_allow_window_am",
            "schedule_hard_cutoff_reason": "late_schedule_start_after_recovery_window_am",
            "duplicate_reason": "skip_duplicate_am_run",
            "duplicate_summary_reason": "phase_guard_skip_duplicate_am_run",
        },
        "pm": {
            "session": "pm",
            "log_prefix": "TRADE_PM",
            "session_start": "1300",
            "session_end_exclusive": "1539",
            "valid_window": "1300-1510",
            "valid_until": "1510",
            "expected_from_env": "PB1_PM_EXPECTED_START",
            "expected_from_legacy_env": "PB1_PM_EXPECTED_START_FROM",
            "expected_from_default": "1305",
            "expected_to_env": "PB1_PM_EXPECTED_END",
            "expected_to_legacy_env": "PB1_PM_EXPECTED_START_TO",
            "expected_to_default": "1315",
            "start_allow_env": "PB1_PM_START_ALLOW_UNTIL",
            "start_allow_default": "1325",
            "recovery_allow_env": "PB1_PM_RECOVERY_ALLOW_UNTIL",
            "recovery_allow_default": "1340",
            "enable_env": "PB1_ENABLE_LATE_PM_RECOVERY",
            "force_late_env": "PB1_FORCE_TRADE_PM_LATE_START",
            "force_hard_env": "PB1_FORCE_TRADE_PM_AFTER_1340",
            "manual_hard_cutoff_reason": "late_manual_start_after_1340_requires_force",
            "schedule_allow_window_reason": "late_schedule_start_after_allow_window_pm",
            "schedule_hard_cutoff_reason": "late_schedule_start_after_recovery_window_pm",
            "duplicate_reason": "skip_duplicate_pm_run",
            "duplicate_summary_reason": "phase_guard_skip_duplicate_pm_run",
        },
    }
    return configs.get(normalized)


def _in_kst_range(now: datetime, start_hhmm: str | None, end_hhmm: str | None, *, end_inclusive: bool = True) -> bool:
    start_dt = _parse_hhmm_kst(start_hhmm, now, "0000")
    end_dt = _parse_hhmm_kst(end_hhmm, now, "2359")
    if end_inclusive:
        return start_dt <= now <= end_dt
    return start_dt <= now < end_dt


def _session_recovery_spec(session_kind: str) -> dict[str, Any] | None:
    return _session_cfg(session_kind)


def _detect_trade_session_recovery_window(*, now: datetime, session_kind: str) -> dict[str, Any]:
    spec = _session_recovery_spec(session_kind)
    if spec is None:
        return {"supported": False, "session_kind": session_kind}

    expected_from = _normalize_hhmm_compact(
        os.getenv(spec["expected_from_env"]) or os.getenv(spec.get("expected_from_legacy_env", "")),
        spec["expected_from_default"],
    )
    expected_to = _normalize_hhmm_compact(
        os.getenv(spec["expected_to_env"]) or os.getenv(spec.get("expected_to_legacy_env", "")),
        spec["expected_to_default"],
    )
    start_allow_until = _normalize_hhmm_compact(os.getenv(spec["start_allow_env"]), spec["start_allow_default"])
    recovery_allow_until = _normalize_hhmm_compact(os.getenv(spec["recovery_allow_env"]), spec["recovery_allow_default"])
    now_hhmm = now.strftime("%H%M")
    late_start = not _in_kst_range(now, "0000", start_allow_until)
    recovery_enabled = parse_bool_any(os.getenv(spec["enable_env"]) or "1", default=True)

    return {
        "supported": True,
        "session_kind": session_kind,
        "log_prefix": spec["log_prefix"],
        "session_start": spec["session_start"],
        "session_end_exclusive": spec["session_end_exclusive"],
        "valid_window": spec["valid_window"],
        "expected_start_window": f"{expected_from}-{expected_to}",
        "start_allow_until": start_allow_until,
        "recovery_allow_until": recovery_allow_until,
        "now_hhmm": now_hhmm,
        "late_start": late_start,
        "within_recovery_window": _in_kst_range(now, start_allow_until, recovery_allow_until),
        "too_late_for_recovery": not _in_kst_range(now, "0000", recovery_allow_until),
        "recovery_enabled": recovery_enabled,
        "duplicate_reason": spec["duplicate_reason"],
    }


def _detect_trade_session_policy(
    *,
    now: datetime,
    event_name: str | None,
    run_mode: str | None,
    trading_day: bool,
    session: str,
) -> dict[str, Any]:
    session_cfg = _session_cfg(session)
    if session_cfg is None:
        return {
            "supported": False,
            "session": session,
            "should_run": True,
            "recovery": False,
            "degraded_session": False,
            "force_override_used": False,
            "classification": "SESSION_POLICY_BYPASS",
            "skip_reason": "",
        }
    event_name_normalized = str(event_name or "schedule").strip().lower() or "schedule"
    session_upper = str(session_cfg["session"]).upper()
    expected_from = _normalize_hhmm_compact(
        os.getenv(session_cfg["expected_from_env"]) or os.getenv(session_cfg.get("expected_from_legacy_env", "")),
        str(session_cfg["expected_from_default"]),
    )
    expected_to = _normalize_hhmm_compact(
        os.getenv(session_cfg["expected_to_env"]) or os.getenv(session_cfg.get("expected_to_legacy_env", "")),
        str(session_cfg["expected_to_default"]),
    )
    start_allow_until = _normalize_hhmm_compact(os.getenv(session_cfg["start_allow_env"]), str(session_cfg["start_allow_default"]))
    recovery_allow_until = _normalize_hhmm_compact(os.getenv(session_cfg["recovery_allow_env"]), str(session_cfg["recovery_allow_default"]))
    recovery_enabled = _env_flag(str(session_cfg["enable_env"]), default=True)
    force_late_start = _env_flag(str(session_cfg["force_late_env"]), default=False)
    force_after_hard_cutoff = _env_flag(str(session_cfg["force_hard_env"]), default=False)

    valid_from = _parse_hhmm_kst(str(session_cfg["session_start"]), now)
    valid_until = _parse_hhmm_kst(str(session_cfg["valid_until"]), now)
    expected_end_dt = _parse_hhmm_kst(expected_to, now, str(session_cfg["expected_to_default"]))
    start_allow_until_dt = _parse_hhmm_kst(start_allow_until, now, str(session_cfg["start_allow_default"]))
    recovery_allow_until_dt = _parse_hhmm_kst(recovery_allow_until, now, str(session_cfg["recovery_allow_default"]))

    policy: dict[str, Any] = {
        "supported": True,
        "session": str(session_cfg["session"]),
        "should_run": True,
        "recovery": False,
        "degraded_session": False,
        "force_override_used": False,
        "manual_force_required": False,
        "skip_reason": "",
        "classification": f"NORMAL_{session_upper}_{'MANUAL' if event_name_normalized == 'workflow_dispatch' else 'SCHEDULE'}",
        "expected_start_window": f"{expected_from}-{expected_to}",
        "start_allow_until": start_allow_until,
        "recovery_allow_until": recovery_allow_until,
        "valid_window": str(session_cfg["valid_window"]),
        "event_name": event_name_normalized,
        "run_mode": str(run_mode or "").strip().lower() or "trade",
        "trading_day": bool(trading_day),
    }

    if policy["run_mode"] != "trade" or not trading_day:
        policy["classification"] = f"BYPASS_{session_upper}_SESSION_POLICY"
        return policy

    if now < valid_from:
        policy.update(
            should_run=False,
            classification=f"SKIP_{session_upper}_EARLY_START",
            skip_reason="early_start",
        )
        return policy
    if now > valid_until:
        if _after_close_entry_dryrun_enabled(now):
            # DIAG+FORCE_RUN+DRY_RUN 조합 → 장마감 후에도 1 tick dry-run 허용
            # 반드시 live gate 강제 차단
            os.environ["LIVE_TRADING_ENABLED"] = "0"
            os.environ["FORCE_BLOCK_LIVE"] = "1"
            policy.update(
                should_run=True,
                recovery=False,
                degraded_session=False,
                force_override_used=True,
                classification=f"AFTER_CLOSE_ENTRY_DRYRUN_{session_upper}",
                skip_reason="after_close_entry_dryrun",
                reason="after_close_entry_dryrun",
            )
            return policy
        policy.update(
            should_run=False,
            classification=f"SKIP_{session_upper}_SESSION_ENDED",
            skip_reason=f"outside_trade_{str(session_cfg['session'])}_valid_window",
            reason=f"outside_trade_{str(session_cfg['session'])}_valid_window",
        )
        return policy

    if event_name_normalized == "workflow_dispatch":
        if now <= recovery_allow_until_dt:
            if now > start_allow_until_dt:
                policy.update(
                    recovery=True,
                    degraded_session=True,
                    classification=f"LATE_{session_upper}_MANUAL_RECOVERY",
                )
            else:
                policy.update(classification=f"NORMAL_{session_upper}_MANUAL")
            return policy
        policy["manual_force_required"] = True
        if force_after_hard_cutoff or force_late_start:
            policy.update(
                should_run=True,
                recovery=True,
                degraded_session=True,
                force_override_used=True,
                classification=f"FORCED_LATE_{session_upper}_MANUAL",
            )
            return policy
        policy.update(
            should_run=False,
            classification=f"SKIP_{session_upper}_MANUAL_HARD_CUTOFF",
            skip_reason=str(session_cfg["manual_hard_cutoff_reason"]),
        )
        return policy

    if now <= start_allow_until_dt:
        policy.update(classification=f"NORMAL_{session_upper}_SCHEDULE")
        return policy
    if now <= recovery_allow_until_dt:
        if not recovery_enabled:
            policy.update(
                should_run=False,
                classification=f"SKIP_{session_upper}_SCHEDULE_ALLOW_WINDOW",
                skip_reason=str(session_cfg["schedule_allow_window_reason"]),
            )
            return policy
        policy.update(
            recovery=True,
            degraded_session=True,
            classification=f"LATE_{session_upper}_RECOVERY",
        )
        return policy
    if force_late_start:
        policy.update(
            should_run=True,
            recovery=True,
            degraded_session=True,
            force_override_used=True,
            classification=f"FORCED_LATE_{session_upper}_SCHEDULE",
        )
        return policy
    policy.update(
        should_run=False,
        classification=f"SKIP_{session_upper}_SCHEDULE_HARD_CUTOFF",
        skip_reason=str(session_cfg["schedule_hard_cutoff_reason"]),
    )
    return policy


def _detect_close_start_policy(*, now: datetime, event_name: str, manual_mode: str = "live_close") -> dict[str, Any]:
    event = str(event_name or "unknown").strip().lower() or "unknown"
    mode = str(manual_mode or "live_close").strip().lower() or "live_close"
    now_hhmm = now.strftime("%H%M")
    if event == "workflow_dispatch" and mode in {"diag_replay", "compute_only"}:
        return {
            "should_run": True,
            "recovery": False,
            "skip_reason": "",
            "classification": "CLOSE_MANUAL_REPLAY",
            "execution_route": "manual_replay",
        }
    if now_hhmm < "1515":
        return {
            "should_run": False,
            "recovery": False,
            "skip_reason": "close_early_start",
            "classification": "SKIP_CLOSE_EARLY_START",
            "execution_route": "live_close",
        }
    if now_hhmm < "1530":
        return {
            "should_run": True,
            "recovery": False,
            "skip_reason": "",
            "classification": "NORMAL_CLOSE_START",
            "execution_route": "live_close",
        }
    if now_hhmm <= "1545":
        return {
            "should_run": True,
            "recovery": True,
            "skip_reason": "",
            "classification": "RECOVERY_CLOSE_START",
            "execution_route": "live_close",
        }
    return {
        "should_run": False,
        "recovery": False,
        "skip_reason": "skip_close_stale_start",
        "classification": "SKIP_CLOSE_STALE_START",
        "execution_route": "live_close",
    }


def _detect_trade_am_start_policy(
    *,
    now: datetime,
    event_name: str | None,
    run_mode: str | None,
    trading_day: bool,
    force_trade_am_late_start: bool,
    force_trade_am_after_1030: bool,
    am_expected_start: str,
    am_expected_end: str,
    am_start_allow_until: str,
    am_recovery_allow_until: str,
) -> dict[str, Any]:
    overrides = {
        "PB1_FORCE_TRADE_AM_LATE_START": "1" if force_trade_am_late_start else "0",
        "PB1_FORCE_TRADE_AM_AFTER_1030": "1" if force_trade_am_after_1030 else "0",
        "PB1_AM_EXPECTED_START": am_expected_start,
        "PB1_AM_EXPECTED_END": am_expected_end,
        "PB1_AM_START_ALLOW_UNTIL": am_start_allow_until,
        "PB1_AM_RECOVERY_ALLOW_UNTIL": am_recovery_allow_until,
    }
    previous = {key: os.getenv(key) for key in overrides}
    try:
        for key, value in overrides.items():
            os.environ[key] = value
        return _detect_trade_session_policy(
            now=now,
            event_name=event_name,
            run_mode=run_mode,
            trading_day=trading_day,
            session="am",
        )
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value



def _compute_session_marker_payload(
    *,
    status: str,
    exit_reason: str | None,
    exit_code: int,
    sell_orders_ack: int = 0,
    entry_status: str = "UNKNOWN",
    entry_abort_reason: str | None = None,
    fatal_error: bool = False,
) -> dict[str, Any]:
    status_u = str(status or "UNKNOWN").upper()
    exit_reason_u = str(exit_reason or "").upper()
    if status_u == "RETRYABLE_ORDER_BUILD_ERROR" or exit_reason_u in {"ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT", "ALL_CANDIDATES_SKIPPED_BEFORE_API_SUBMIT", "RETRYABLE_ORDER_BUILD_ERROR"}:
        return {
            "status": "RETRYABLE_ORDER_BUILD_ERROR",
            "exit_reason": "ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT" if exit_reason_u != "RETRYABLE_ORDER_BUILD_ERROR" else str(exit_reason),
            "completed": False,
            "retryable": True,
            "sell_orders_ack": int(sell_orders_ack or 0),
            "sell_completed": bool(int(sell_orders_ack or 0) > 0),
            "entry_status": str(entry_status or "UNKNOWN").upper(),
            "entry_abort_reason": entry_abort_reason,
            "entry_completed": False,
        }
    entry_status_u = str(entry_status or "UNKNOWN").upper()
    entry_done = entry_status_u in {"DONE", "OK", "OK_NO_TRADE", "SKIPPED_BY_POLICY"} and not entry_abort_reason
    completed = int(
        int(exit_code) == 0
        and status_u in {"OK", "OK_NO_TRADE", "PB1_SESSION_DONE"}
        and entry_done
        and not fatal_error
    )
    sell_ack = int(sell_orders_ack or 0)
    marker_status = "PARTIAL_SUCCESS_RETRYABLE" if sell_ack > 0 and not completed else (status_u if completed or status_u not in {"", "UNKNOWN"} else "RETRYABLE_FAILURE")
    return {
        "status": marker_status,
        "exit_reason": exit_reason,
        "completed": bool(completed),
        "retryable": bool(not completed),
        "sell_orders_ack": sell_ack,
        "sell_completed": bool(sell_ack > 0),
        "entry_status": entry_status_u,
        "entry_abort_reason": entry_abort_reason,
        "entry_completed": bool(entry_done),
    }


@dataclass
class NormalizedSessionResult:
    status: str
    reason: str
    completed: int
    retryable: int
    exit_reason: str


def normalize_session_result(
    *,
    status: str,
    reason: str,
    order_candidates: int = 0,
    api_submitted: int = 0,
    skipped: int = 0,
    skip_reasons: list[str] | None = None,
) -> NormalizedSessionResult:
    status_u = str(status or "UNKNOWN").upper()
    reason_u = str(reason or "").upper()
    skip_reason_set = {str(r or "").upper() for r in (skip_reasons or [])}
    policy_block_reasons = {
        "BUYABLE_EXISTING_HOLDING_KIS",
        "BUYABLE_TODAY_BUY_EXISTS",
        "BUYABLE_TODAY_SELL_REBUY_BLOCKED",
        "BUYABLE_COOLDOWN",
        "BUYABLE_DUPLICATE",
        "MARKET_RISK_OFF_ENTRY_BLOCK",
        "SECTOR_CAP_BLOCK",
        "GROSS_EXPOSURE_CAP",
        "CASH_INSUFFICIENT",
        "MAX_POSITIONS_REACHED",
    }
    if (
        reason_u in policy_block_reasons
        or "BUYABLE_EXISTING_HOLDING_KIS" in reason_u
        or bool(skip_reason_set & policy_block_reasons)
    ):
        return NormalizedSessionResult(
            status="OK_NO_TRADE",
            reason=str(reason or next(iter(skip_reason_set & policy_block_reasons), reason_u)),
            completed=1,
            retryable=0,
            exit_reason=str(reason or next(iter(skip_reason_set & policy_block_reasons), reason_u)),
        )
    retryable_tokens = {
        "ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT",
        "ORDER_CANDIDATE_WITHOUT_API_SUBMIT",
    }
    plan_skip_reasons = {
        r for r in skip_reason_set
        if "ENTRY_PLAN" in r or "ENTRY_ORDER_PLAN" in r or "ENTRY_EXIT_PLAN" in r
    }
    retryable_order_build_error = (
        status_u in {"OK_NO_TRADE", "RETRYABLE_ORDER_BUILD_ERROR"}
        and int(order_candidates or 0) > 0
        and int(api_submitted or 0) == 0
        and bool(plan_skip_reasons)
    ) or status_u == "RETRYABLE_ORDER_BUILD_ERROR" or reason_u in retryable_tokens
    if retryable_order_build_error:
        return NormalizedSessionResult(
            status="RETRYABLE_ORDER_BUILD_ERROR",
            reason=str(reason or "ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT"),
            completed=0,
            retryable=1,
            exit_reason="ENTRY_PLAN_INVALID_BEFORE_API_SUBMIT",
        )
    completed = int(status_u in {"OK", "OK_NO_TRADE", "PB1_SESSION_DONE"})
    return NormalizedSessionResult(
        status=status_u,
        reason=str(reason or ""),
        completed=completed,
        retryable=int(not completed),
        exit_reason=str(reason or ""),
    )


PM_RETRYABLE_NO_TRADE_STATUSES = {"OK_NO_TRADE"}
PM_RETRYABLE_NO_TRADE_REASONS = {
    "NO_ORDERABLE_CANDIDATES",
    "NO_CANDIDATES_AFTER_RELAX",
    "NO_FINAL_SETUPS",
    "NO_FINAL_SETUPS_AFTER_STYLE_GATE",
    "NO_BRIDGE_CANDIDATES",
}


def _is_pm_session(session: str | None) -> bool:
    return str(session or "").strip().lower() in {"afternoon", "pm"}


def normalize_pm_no_trade_marker(payload: dict[str, Any] | None, session: str | None = None) -> dict[str, Any]:
    marker = dict(payload or {})
    status = str(marker.get("status") or "").upper()
    reason = str(marker.get("reason") or marker.get("exit_reason") or marker.get("entry_abort_reason") or "").upper()
    if parse_bool_any(os.getenv("PB1_PM_NO_TRADE_RETRYABLE"), default=True) and _is_pm_session(session):
        if status in PM_RETRYABLE_NO_TRADE_STATUSES and reason in PM_RETRYABLE_NO_TRADE_REASONS:
            before_completed = bool(marker.get("completed"))
            before_retryable = bool(marker.get("retryable"))
            marker["completed"] = False
            marker["retryable"] = True
            logger.info(
                "[TRADE_PM][DEDUPE][NORMALIZE] marker_status=%s marker_reason=%s before_completed=%s before_retryable=%s after_completed=0 after_retryable=1",
                status,
                reason,
                int(before_completed),
                int(before_retryable),
            )
    return marker


def should_skip_duplicate_from_marker(payload: dict[str, Any] | None, session: str | None = None) -> bool:
    payload = normalize_pm_no_trade_marker(payload, session=session)
    payload = payload or {}
    marker_completed = bool(payload.get("completed"))
    marker_retryable = bool(payload.get("retryable"))
    marker_status = str(payload.get("status") or "").upper()
    return bool(
        marker_completed
        and not marker_retryable
        and marker_status not in {
            "RETRYABLE_ORDER_BUILD_ERROR",
            "PARTIAL_SUCCESS_RETRYABLE",
            "RETRYABLE_FAILURE",
        }
    )


def _pm_tick_bucket(now_kst: datetime) -> str:
    minute = 0 if int(now_kst.minute) < 30 else 30
    return f"{int(now_kst.hour):02d}{minute:02d}"

def _write_session_result_file(payload: dict[str, Any]) -> None:
    path = os.getenv("PB1_SESSION_RESULT_PATH")
    if not path:
        return
    try:
        buy_orders = int(payload.get("buy_orders", 0) or 0)
        sell_orders = int(payload.get("sell_orders", 0) or 0)
        buy_orders_ack = int(payload.get("buy_orders_ack", payload.get("accepted", buy_orders)) or 0)
        sell_orders_ack = int(payload.get("sell_orders_ack", sell_orders) or 0)
        api_submitted = int(payload.get("api_submitted", buy_orders_ack + sell_orders_ack) or 0)
        order_candidates = int(payload.get("order_candidates", payload.get("entry_candidates", 0)) or 0)
        ticks_total = int(payload.get("ticks_total", 0) or 0)
        payload["buy_orders"] = buy_orders
        payload["sell_orders"] = sell_orders
        payload["buy_orders_ack"] = max(0, min(buy_orders_ack, buy_orders if buy_orders > 0 else buy_orders_ack))
        payload["sell_orders_ack"] = max(0, min(sell_orders_ack, sell_orders if sell_orders > 0 else sell_orders_ack))
        payload["api_submitted"] = max(0, api_submitted)
        payload["order_candidates"] = max(0, order_candidates)
        payload["ticks_total"] = max(0, ticks_total)
        if int(payload.get("api_submitted", 0) or 0) > 0 and str(payload.get("status") or "").upper() == "OK_NO_TRADE":
            payload["status"] = "OK_WITH_ORDERS"

        required_defaults = {
            "status": "UNKNOWN",
            "exit_reason": "",
            "terminal_state": "",
            "ticks_total": 0,
            "buy_orders": 0,
            "buy_orders_ack": 0,
            "sell_orders": 0,
            "sell_orders_ack": 0,
            "rejected_orders": 0,
            "skipped_orders": 0,
            "exit_evaluated_positions": 0,
            "entry_candidates": 0,
            "order_candidates": 0,
            "api_submitted": 0,
            "exit_submitted_codes": [],
            "no_sellable_qty_terminal_codes": [],
            "fatal_error_type": "",
            "fatal_error_message": "",
            "warning_counts": {},
        }
        for key, value in required_defaults.items():
            payload.setdefault(key, value)
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        logger.info(
            "[PB1][SESSION_RESULT][WRITE_OK] path=%s status=%s sell_orders_ack=%s buy_orders=%s",
            path, payload.get("status"), payload.get("sell_orders_ack"), payload.get("buy_orders"),
        )
    except Exception as exc:
        logger.warning("[PB1][SESSION_RESULT][WRITE_WARN] path=%s err=%s", path, exc)

def _session_execution_checkpoint_key(*, env: str, session_kind: str, trade_date: date, now_kst: datetime | None = None) -> str:
    session_norm = str(session_kind or "").strip().lower()
    key = f"trade_session:{str(env or '').strip().lower() or 'practice'}:{session_norm}:{trade_date.isoformat()}"
    if parse_bool_any(os.getenv("PB1_PM_CHECKPOINT_TICK_BUCKET"), default=True) and _is_pm_session(session_norm):
        now_for_bucket = now_kst or _get_now_kst()
        key = f"{key}:tick={_pm_tick_bucket(now_for_bucket)}"
    return key


def _close_reconcile_checkpoint_key(*, env: str, trade_date: date) -> str:
    return f"close_reconcile:{str(env or '').strip().lower() or 'practice'}:{trade_date.isoformat()}"


def _record_session_execution_marker(
    *,
    engine,
    env: str,
    session_kind: str,
    trade_date: date,
    status: str,
    exit_reason: str | None,
    recovery_used: bool,
    completed: bool | None = None,
    retryable: bool | None = None,
    sell_orders_ack: int = 0,
    entry_status: str = "UNKNOWN",
    entry_abort_reason: str | None = None,
    exit_code: int = 0,
) -> None:
    spec = _session_recovery_spec(session_kind)
    if spec is None:
        return
    marker = _compute_session_marker_payload(
        status=status,
        exit_reason=exit_reason,
        exit_code=exit_code,
        sell_orders_ack=sell_orders_ack,
        entry_status=entry_status,
        entry_abort_reason=entry_abort_reason,
        fatal_error=str(status or "").upper() == "ERROR",
    )
    if completed is not None:
        marker["completed"] = bool(completed)
    if retryable is not None:
        marker["retryable"] = bool(retryable)
    session_norm = str(session_kind or "").strip().lower()
    status_u = str(marker.get("status") or status or "").upper()
    reason_u = str(exit_reason or marker.get("exit_reason") or marker.get("entry_abort_reason") or "").upper()
    if parse_bool_any(os.getenv("PB1_PM_NO_TRADE_RETRYABLE"), default=True) and _is_pm_session(session_norm):
        if status_u in PM_RETRYABLE_NO_TRADE_STATUSES and reason_u in PM_RETRYABLE_NO_TRADE_REASONS:
            marker["completed"] = False
            marker["retryable"] = True
    payload = {
        "env": str(env or "").strip().lower() or "practice",
        "session_kind": str(session_kind or "").strip().lower(),
        "trade_date": trade_date.isoformat(),
        "status": marker["status"],
        "exit_reason": exit_reason,
        "recovery_used": bool(recovery_used),
        "completed": bool(marker["completed"]),
        "retryable": bool(marker["retryable"]),
        "sell_orders_ack": int(marker["sell_orders_ack"]),
        "sell_orders_ack_source": "pb1_result_file" if os.getenv("PB1_SESSION_RESULT_PATH") else "missing_result_file",
        "pb1_result_path": str(os.getenv("PB1_SESSION_RESULT_PATH") or ""),
        "sell_completed": bool(marker["sell_completed"]),
        "entry_status": marker["entry_status"],
        "entry_abort_reason": marker["entry_abort_reason"],
        "entry_completed": bool(marker["entry_completed"]),
        "updated_at": _get_now_kst().isoformat(),
    }
    try:
        save_job_checkpoint(
            engine,
            _session_execution_checkpoint_key(env=env, session_kind=session_kind, trade_date=trade_date),
            payload,
        )
        logger.info(
            "[%s][DEDUPE] source=job_checkpoint completed=%s status=%s retryable=%s exit_reason=%s recovery_used=%s",
            spec["log_prefix"],
            int(bool(payload["completed"])),
            payload["status"],
            int(bool(payload["retryable"])),
            exit_reason,
            int(bool(recovery_used)),
        )
    except Exception as exc:
        logger.warning(
            "[%s][DEDUPE] source=job_checkpoint save_failed=1 err=%s",
            spec["log_prefix"],
            exc,
        )


def _has_today_session_buy_activity(orders_repo: OrdersRepo, env: str, now: datetime, session: str) -> dict[str, Any]:
    list_today_session_buy_orders = getattr(orders_repo, "list_today_session_buy_orders", None)
    if callable(list_today_session_buy_orders):
        buy_orders = list_today_session_buy_orders(env, session, trade_date=now.date())
    else:
        session_cfg = _session_cfg(session) or _session_cfg("am")
        session_start_dt = _parse_hhmm_kst(str(session_cfg["session_start"]), now)
        session_end_dt = _parse_hhmm_kst(str(session_cfg["session_end_exclusive"]), now)
        buy_orders = orders_repo.list_today_buy_orders(
            env,
            start_at=session_start_dt,
            end_at=session_end_dt,
        )
    get_open_orders = getattr(orders_repo, "get_open_orders", None)
    open_orders = get_open_orders(env) if callable(get_open_orders) else []
    open_buy_exists = any(str(order.get("side") or "").upper() == "BUY" for order in open_orders)
    marker_payload: dict[str, Any] = {}
    get_marker_payload = getattr(orders_repo, "get_today_session_marker_payload", None)
    if callable(get_marker_payload):
        marker_payload = get_marker_payload(env, session, now.date()) or {}
        marker_payload = normalize_pm_no_trade_marker(marker_payload, session=session)
        session_completed_marker = bool(marker_payload.get("completed"))
    else:
        has_today_session_marker = getattr(orders_repo, "has_today_session_marker", None)
        if callable(has_today_session_marker):
            session_completed_marker = bool(has_today_session_marker(env, session, "completed", now.date()))
            marker_payload = {"completed": session_completed_marker, "status": "UNKNOWN", "retryable": False}
            marker_payload = normalize_pm_no_trade_marker(marker_payload, session=session)
        else:
            has_today_am_session_marker = getattr(orders_repo, "has_today_am_session_marker", None)
            session_completed_marker = bool(has_today_am_session_marker(env, now.date())) if callable(has_today_am_session_marker) and session == "am" else False
            marker_payload = {"completed": session_completed_marker, "status": "UNKNOWN", "retryable": False}
            marker_payload = normalize_pm_no_trade_marker(marker_payload, session=session)
    session_buy_exists = bool(buy_orders)
    duplicate_by_marker = should_skip_duplicate_from_marker(marker_payload, session=session)
    duplicate = session_buy_exists or open_buy_exists or duplicate_by_marker
    if session_buy_exists:
        reason = "session_buy_exists"
    elif open_buy_exists:
        reason = "open_buy_exists"
    elif session_completed_marker:
        reason = "session_completed_marker" if duplicate_by_marker else "no_prior_session_buy"
    else:
        reason = "no_prior_session_buy"
    return {
        "session_buy_exists": session_buy_exists,
        "open_buy_exists": open_buy_exists,
        "session_completed_marker": session_completed_marker,
        "marker_status": str(marker_payload.get("status") or "").upper(),
        "marker_retryable": bool(marker_payload.get("retryable")),
        "marker_completed": bool(marker_payload.get("completed")),
        "duplicate": duplicate,
        "reason": reason,
    }


def _resolve_reconcile_only_followup(
    *,
    enabled: bool,
    pending: bool,
    buy_orders: int,
    sell_orders: int,
    open_orders_count: int,
) -> tuple[bool, bool]:
    if not enabled:
        return False, False
    if pending:
        return True, open_orders_count > 0
    return False, (buy_orders + sell_orders) > 0


def _apply_trade_session_override_env(*, enabled: bool, degraded_session: bool, session: str, classification: str) -> None:
    os.environ["PB1_FORCE_ENTRY_WINDOW_OVERRIDE"] = "1" if enabled else "0"
    os.environ["PB1_SESSION_RECOVERY_CONTINUE"] = "1" if enabled else "0"
    os.environ["PB1_AM_RECOVERY_CONTINUE"] = "1" if enabled and session == "am" else "0"
    os.environ["PB1_PM_RECOVERY_CONTINUE"] = "1" if enabled and session == "pm" else "0"
    os.environ["PB1_FORCED_TRADE_SESSION"] = session if enabled else ""
    os.environ["PB1_PHASE_GUARD_CLASSIFICATION"] = classification or ""
    os.environ["PB1_SESSION_RECOVERY_USED"] = "1" if enabled else "0"
    os.environ["TRADE_AM_LATE_START"] = "1" if enabled and session == "am" else "0"
    os.environ["TRADE_AM_DEGRADED_SESSION"] = "1" if degraded_session and session == "am" else "0"
    os.environ["TRADE_PM_LATE_START"] = "1" if enabled and session == "pm" else "0"
    os.environ["TRADE_PM_DEGRADED_SESSION"] = "1" if degraded_session and session == "pm" else "0"


def _evaluate_trade_session_start_guard(*, engine, env: str, now: datetime, session: str) -> dict[str, Any]:
    if session == "close" or (os.getenv("FORCE_PB1_PHASE") or "").strip().lower() == "close":
        logger.info("[PB1][PHASE] phase=close reason=forced_close_session entry_enabled=0 exit_enabled=1 close_enabled=1")
        return {"skip": False, "recovery_used": False, "force_override_used": True, "exit_reason": "forced_close_session", "policy": {"should_run": True}}
    session_cfg = _session_cfg(session)
    if session_cfg is None:
        return {"skip": False, "recovery_used": False, "exit_reason": "none"}
    event_name = os.getenv("GITHUB_EVENT_NAME") or "schedule"
    run_mode = os.getenv("MODE") or os.getenv("MANUAL_MODE") or "trade"
    policy = _detect_trade_session_policy(
        now=now,
        event_name=event_name,
        run_mode=run_mode,
        trading_day=is_trading_day(now),
        session=session,
    )
    recovery_enabled = bool(policy["recovery"] or policy["force_override_used"])
    _apply_trade_session_override_env(
        enabled=recovery_enabled,
        degraded_session=bool(policy["degraded_session"]),
        session=session,
        classification=str(policy["classification"]),
    )

    phase_guard_parts = [
        f"[{session_cfg['log_prefix']}][PHASE_GUARD] now_kst={now.strftime('%H%M')}",
        f"should_run={int(bool(policy['should_run']))}",
        f"recovery={int(bool(policy['recovery']))}",
        f"force_override_used={int(bool(policy['force_override_used']))}",
        f"reason={policy.get('reason') or policy['skip_reason']}",
        f"skip_reason={policy['skip_reason']}",
        f"event={policy['event_name']}",
        f"classification={policy['classification']}",
    ]
    logger.warning(" ".join(phase_guard_parts))

    if not policy["should_run"]:
        _apply_trade_session_override_env(enabled=False, degraded_session=False, session=session, classification=str(policy["classification"]))
        return {
            "skip": True,
            "recovery_used": False,
            "force_override_used": bool(policy["force_override_used"]),
            "exit_reason": str(policy["skip_reason"] or "phase_guard_skip"),
            "policy": policy,
        }

    if not recovery_enabled:
        return {
            "skip": False,
            "recovery_used": False,
            "force_override_used": bool(policy["force_override_used"]),
            "exit_reason": "none",
            "policy": policy,
        }

    orders_repo = OrdersRepo(engine)
    duplicate_activity = _has_today_session_buy_activity(orders_repo, env, now, session)
    logger.warning(
        "[%s][DEDUPE] session_buy_exists=%s open_buy_exists=%s session_completed_marker=%s marker_status=%s marker_retryable=%s marker_completed=%s result=%s",
        session_cfg["log_prefix"],
        int(bool(duplicate_activity["session_buy_exists"])),
        int(bool(duplicate_activity["open_buy_exists"])),
        int(bool(duplicate_activity["session_completed_marker"])),
        duplicate_activity.get("marker_status") or "NONE",
        int(bool(duplicate_activity.get("marker_retryable"))),
        int(bool(duplicate_activity.get("marker_completed"))),
        "skip_duplicate" if duplicate_activity["duplicate"] else ("continue" if duplicate_activity.get("marker_retryable") else "proceed"),
    )
    if duplicate_activity["duplicate"]:
        _apply_trade_session_override_env(
            enabled=False,
            degraded_session=bool(policy["degraded_session"]),
            session=session,
            classification=str(policy["classification"]),
        )
        return {
            "skip": True,
            "recovery_used": bool(policy["recovery"]),
            "force_override_used": bool(policy["force_override_used"]),
            "exit_reason": str(session_cfg["duplicate_reason"]),
            "policy": policy,
            "duplicate_activity": duplicate_activity,
        }
    return {
        "skip": False,
        "recovery_used": bool(policy["recovery"]),
        "force_override_used": bool(policy["force_override_used"]),
        "exit_reason": "recovery_continue" if policy["recovery"] else "none",
        "policy": policy,
        "duplicate_activity": duplicate_activity,
    }


def _evaluate_session_recovery_guard(*, engine, env: str, session_kind: str, now: datetime) -> dict[str, Any]:
    if str(session_kind or "").strip().lower() not in {"am", "pm"}:
        return {"skip": False, "recovery_used": False, "exit_reason": "none"}
    return _evaluate_trade_session_start_guard(
        engine=engine,
        env=env,
        now=now,
        session=str(session_kind).strip().lower(),
    )


def _apply_afternoon_mode_decision(*, run_ctx: dict[str, Any], entry_enabled: bool, exit_only: bool) -> dict[str, Any]:
    if entry_enabled:
        os.environ["PB1_ENTRY_ENABLED"] = "1"
        os.environ["PB1_EXIT_ONLY_MODE"] = "0"
        os.environ["PB1_PHASE_DEFAULT"] = "pm_entry"
        os.environ["FORCE_PB1_PHASE"] = "pm_entry"
        os.environ.pop("FORCE_ENTRY_DISABLED_REASON", None)
        run_ctx["phase_name"] = "pm_entry"
        run_ctx["exit_only"] = False
        run_ctx["entry_enabled"] = True
        logger.info("[TRADE_AFTERNOON][MODE_APPLIED] entry_enabled=1 exit_only=0 phase=pm_entry source=db_prep_final30")
        return {"phase": "pm_entry", "entry_enabled": True, "exit_only": False}
    run_ctx["exit_only"] = bool(exit_only)
    run_ctx["entry_enabled"] = False
    return {"phase": str(run_ctx.get("phase_name") or ""), "entry_enabled": False, "exit_only": bool(exit_only)}


def _resolve_session_trade_policy(*, session: str, phase_name: str, entry_enabled: bool, now: datetime | None = None) -> dict[str, Any]:
    normalized_session = str(session or "").strip().lower()
    normalized_phase = str(phase_name or "").strip().lower()
    if normalized_session not in {"pm", "afternoon"}:
        return {"no_new_entry": False, "reason": ""}
    late_start_action = str(os.getenv("PB1_PM_LATE_START_ACTION") or "ALLOW_BEFORE_CUTOFF").strip().upper()
    phase_guard_classification = str(os.getenv("PB1_PHASE_GUARD_CLASSIFICATION") or "").strip().upper()
    current_now = now or _get_now_kst()
    entry_cutoff_raw = (os.getenv("ENTRY_CUTOFF_TIME") or PB1_ENTRY_WINDOW_END or "15:15").strip()
    market_close_raw = (os.getenv("MARKET_CLOSE_TIME") or CLOSE_AUCTION_END or "15:30").strip()
    try:
        entry_cutoff_time = datetime.strptime(entry_cutoff_raw, "%H:%M").time()
    except ValueError:
        entry_cutoff_time = datetime.strptime("15:15", "%H:%M").time()
    try:
        market_close_time = datetime.strptime(market_close_raw, "%H:%M").time()
    except ValueError:
        market_close_time = datetime.strptime("15:30", "%H:%M").time()
    late_start_detected = (
        _env_flag("TRADE_PM_LATE_START", default=False)
        or "LATE_PM" in phase_guard_classification
        or "FORCED_LATE_PM" in phase_guard_classification
    )
    if not entry_enabled or normalized_phase in {"manage", "exit", "idle"}:
        return {"no_new_entry": True, "reason": "pm_strategy_manage_only"}
    if current_now.time() >= market_close_time:
        return {"no_new_entry": True, "reason": "MARKET_CLOSED"}
    pm_close_raw = (os.getenv("PB1_PM_SESSION_END") or os.getenv("PM_SESSION_END") or "15:10").strip()
    try:
        pm_close_time = datetime.strptime(pm_close_raw, "%H:%M").time()
    except ValueError:
        pm_close_time = datetime.strptime("15:10", "%H:%M").time()
    if current_now.time() >= pm_close_time:
        return {"no_new_entry": True, "reason": "close_window"}
    if _env_flag("PB1_BLOCK_ENTRY_AFTER_CUTOFF", default=False) and current_now.time() >= entry_cutoff_time:
        logger.warning(
            "[TRADE_AFTERNOON][ENTRY_ALLOW_UNTIL_PASSED][WARN_ONLY] now=%s allow_until=%s entry_still_enabled=%s",
            current_now.strftime("%H:%M"),
            entry_cutoff_raw,
            int(bool(entry_enabled)),
        )
        return {"no_new_entry": False, "reason": "ENTRY_CUTOFF_PASSED_WARN_ONLY", "warning_only": True}
    if late_start_detected and late_start_action == "ALLOW_BEFORE_CUTOFF":
        return {"no_new_entry": False, "reason": "LATE_START_BEFORE_CUTOFF", "warning_only": True}
    return {"no_new_entry": False, "reason": ""}


def _normalize_phase_guard_exit_reason(session: str, exit_reason: str) -> str:
    session_cfg = _session_cfg(session)
    if session_cfg is None:
        return exit_reason
    if exit_reason == str(session_cfg["duplicate_reason"]):
        return str(session_cfg["duplicate_summary_reason"])
    return exit_reason


def _is_manual_session_restart(event_name: str | None, workflow_attempt: int | None) -> bool:
    normalized_event = str(event_name or "").strip().lower()
    if normalized_event == "workflow_dispatch":
        return True
    return int(workflow_attempt or 0) > 1


def _resolve_session_guard_stale_sec(*, event_name: str | None, workflow_attempt: int | None) -> tuple[int, bool]:
    default_stale_sec = max(1, _parse_int_env("PB1_SESSION_GUARD_STALE_SEC", 600))
    manual_restart = _is_manual_session_restart(event_name, workflow_attempt)
    allow_manual_takeover = env_bool("PB1_ALLOW_MANUAL_SESSION_TAKEOVER", default=True)
    if manual_restart and allow_manual_takeover:
        manual_stale_sec = max(1, _parse_int_env("PB1_MANUAL_SESSION_GUARD_STALE_SEC", 60))
        return min(default_stale_sec, manual_stale_sec), True
    return default_stale_sec, False


def _touch_session_guard(
    *,
    runs_repo: RunsRepo,
    session_guard_run_id: str | None,
    session_kind: str,
    status: str = "SESSION_GUARD_RUNNING",
) -> None:
    if not session_guard_run_id:
        return
    now = _get_now_kst()
    try:
        runs_repo.touch_run(session_guard_run_id, now=now, status=status)
        logger.info(
            "[PB1][SESSION_GUARD][HEARTBEAT] run_id=%s kind=%s now=%s status=%s",
            session_guard_run_id,
            session_kind,
            now.isoformat(),
            status,
        )
    except Exception as exc:
        logger.warning(
            "[PB1][SESSION_GUARD][HEARTBEAT_FAIL] run_id=%s kind=%s err=%s",
            session_guard_run_id,
            session_kind,
            exc,
        )


def _acquire_session_guard_or_takeover(
    *,
    runs_repo: RunsRepo,
    env: str,
    session_kind: str,
    run_id: str,
    workflow_run_id: str | None,
    workflow_attempt: int | None,
    event_name: str | None,
    workflow: str | None,
    git_sha: str | None,
    strategy_env: str | None,
    kis_env: str | None,
    ctx_env: str | None,
) -> dict[str, Any]:
    spec = _session_guard_spec(session_kind)
    if spec is None:
        return {
            "blocked": False,
            "fail_open": False,
            "session_guard_run_id": None,
        }

    guard_env = resolve_env(env)
    log_prefix = str(spec["log_prefix"])
    strategy = str(spec["strategy"])
    run_window = str(spec["run_window"])
    phase = str(spec["phase"])
    stale_sec, manual_takeover_enabled = _resolve_session_guard_stale_sec(
        event_name=event_name,
        workflow_attempt=workflow_attempt,
    )
    manual_restart = _is_manual_session_restart(event_name, workflow_attempt)

    logger.info(
        "[PB1][SESSION_GUARD][ENV] strategy_env=%s kis_env=%s ctx_env=%s guard_env=%s",
        str(strategy_env or "").strip().lower() or "",
        str(kis_env or "").strip().lower() or "",
        str(ctx_env or "").strip().lower() or "",
        guard_env,
    )
    if str(ctx_env or "").strip().lower() != guard_env:
        logger.warning(
            "[PB1][SESSION_GUARD][ENV_MISMATCH] strategy_env=%s kis_env=%s ctx_env=%s guard_env=%s",
            str(strategy_env or "").strip().lower() or "",
            str(kis_env or "").strip().lower() or "",
            str(ctx_env or "").strip().lower() or "",
            guard_env,
        )

    try:
        existing_session = runs_repo.find_active_session_today(
            env=guard_env,
            strategy=strategy,
            run_window=run_window,
            phase=phase,
        )
        if existing_session is None and session_kind == "am":
            existing_session = runs_repo.find_active_session_today(
                env=guard_env,
                strategy="pb1_pullback_close",
                run_window=run_window,
                phase=phase,
            )
    except Exception as exc:
        logger.exception("[PB1][SESSION_GUARD][FAIL] continuing_without_existing_session_check err=%s", exc)
        if _fail_open_on_runs_ledger_error(guard_env):
            logger.warning(
                "[PB1][SESSION_GUARD][FAIL_OPEN] env=%s strategy=%s run_window=%s phase=%s",
                guard_env,
                strategy,
                run_window,
                phase,
            )
            logger.warning(
                "[RUN_SUMMARY][WARN] reason=runs_ledger_fail_open session=%s event=%s",
                session_kind,
                str(event_name or "unknown"),
            )
            existing_session = None
            fail_open = True
        else:
            raise RuntimeError("RUNS_LEDGER_QUERY_FAIL") from exc
    else:
        fail_open = False

    takeover_from_run_id: str | None = None
    if existing_session:
        existing_run_id = str(existing_session.get("run_id") or "") or None
        existing_workflow_run_id = str(existing_session.get("workflow_run_id") or "") or None
        stale = runs_repo.is_session_stale(existing_session, stale_sec=stale_sec)
        if existing_session.get("finished_at") or RunsRepo._is_terminal_status(existing_session.get("status")):
            logger.info(
                "[%s][DUPLICATE_GUARD][FINISHED_IGNORE] old_run_id=%s status=%s",
                log_prefix,
                existing_run_id,
                existing_session.get("status"),
            )
        elif stale:
            takeover_reason = "stale_session_takeover"
            takeover_status = "SESSION_STALE_TAKEOVER"
            log_action = "STALE_TAKEOVER"
            if manual_restart and manual_takeover_enabled and existing_workflow_run_id != workflow_run_id:
                takeover_reason = "manual_session_takeover"
                takeover_status = "SESSION_MANUAL_TAKEOVER"
                log_action = "MANUAL_TAKEOVER"
            runs_repo.finish_stale_session_if_needed(
                existing_session,
                stale_sec=stale_sec,
                reason=takeover_reason,
                takeover_from_run_id=None,
                status=takeover_status,
            )
            logger.warning(
                "[%s][DUPLICATE_GUARD][%s] old_run_id=%s stale_sec=%s new_workflow_run_id=%s event=%s",
                log_prefix,
                log_action,
                existing_run_id,
                stale_sec,
                workflow_run_id,
                str(event_name or "unknown"),
            )
            takeover_from_run_id = existing_run_id
        else:
            logger.warning(
                "[%s][DUPLICATE_GUARD][ACTIVE_BLOCK] existing_run_id=%s started_at=%s heartbeat_at=%s workflow_run_id=%s",
                log_prefix,
                existing_run_id,
                existing_session.get("started_at"),
                existing_session.get("heartbeat_at") or existing_session.get("updated_at"),
                existing_workflow_run_id,
            )
            return {
                "blocked": True,
                "fail_open": fail_open,
                "session_guard_run_id": None,
            }

    session_guard_run_id = runs_repo.create_session_guard(
        env=guard_env,
        strategy=strategy,
        run_window=run_window,
        phase=phase,
        event_name=str(event_name or "").strip().lower() or "schedule",
        workflow=workflow,
        workflow_run_id=workflow_run_id,
        workflow_attempt=workflow_attempt,
        git_sha=git_sha,
        takeover_from_run_id=takeover_from_run_id,
        config_json={
            "session_kind": session_kind,
            "run_id": run_id,
            "expected_start_window": spec["expected_start_window"],
            "manual_restart": manual_restart,
            "stale_sec": stale_sec,
        },
    )
    logger.info(
        "[%s][DUPLICATE_GUARD] session_guard_started=1 run_id=%s session_kind=%s",
        log_prefix,
        session_guard_run_id,
        session_kind,
    )
    return {
        "blocked": False,
        "fail_open": fail_open,
        "session_guard_run_id": session_guard_run_id,
    }


def _finish_session_guard(
    *,
    runs_repo: RunsRepo,
    session_guard_run_id: str | None,
    session_kind: str,
    exit_reason: str | None,
    last_phase: str,
) -> None:
    if not session_guard_run_id:
        return
    spec = _session_guard_spec(session_kind) or {}
    log_prefix = str(spec.get("log_prefix") or "TRADE")
    status = f"SESSION_{str(exit_reason or 'UNKNOWN').upper()}"
    aborted_reason = str(exit_reason or "unknown").strip().lower() or None
    try:
        runs_repo.finish_run(
            session_guard_run_id,
            status=status,
            notes=last_phase,
            aborted_reason=aborted_reason,
        )
    except Exception as exc:
        logger.warning("[%s][DUPLICATE_GUARD][FINISH_FAIL] run_id=%s err=%s", log_prefix, session_guard_run_id, exc)
        try:
            runs_repo.mark_run_abandoned(
                session_guard_run_id,
                reason=f"finish_fallback:{aborted_reason or 'unknown'}",
                status=status,
            )
        except Exception as fallback_exc:
            logger.warning(
                "[%s][DUPLICATE_GUARD][FINISH_FALLBACK_FAIL] run_id=%s err=%s",
                log_prefix,
                session_guard_run_id,
                fallback_exc,
            )


def _resolve_session_exit_grace_sec() -> int:
    try:
        return max(0, int(os.getenv("PB1_SESSION_EXIT_GRACE_SEC", "15")))
    except Exception:
        return 15


class TickTimeoutError(TimeoutError):
    pass


def _run_once_with_hard_timeout(*, timeout_sec: int, call):
    if timeout_sec <= 0:
        last_stage = str(os.getenv("PB1_LAST_STAGE") or "unknown")
        raise TickTimeoutError(f"tick_hard_timeout timeout_sec={timeout_sec} last_stage={last_stage}")

    def _alarm_handler(signum, frame):
        last_stage = str(os.getenv("PB1_LAST_STAGE") or "unknown")
        raise TickTimeoutError(f"tick_hard_timeout timeout_sec={timeout_sec} last_stage={last_stage}")

    prev = signal.getsignal(signal.SIGALRM)
    try:
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.setitimer(signal.ITIMER_REAL, float(timeout_sec))
        return call()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prev)


def _sleep_until_next_tick_or_session_end(now: datetime, session_end_dt: datetime, loop_interval: int) -> float:
    remaining = (session_end_dt - now).total_seconds()
    if remaining <= 0:
        return 0.0
    return max(1.0, min(float(loop_interval), remaining))

_WINDOW_MISMATCH_LOGGED = False
BALANCE_STATE_OK = "OK"
BALANCE_STATE_STALE_OK = "STALE_OK"
BALANCE_STATE_UNKNOWN = "UNKNOWN"
DEFAULT_UNIVERSE_STRATEGY = "best_k_meta"
_MIGRATIONS_BOOTSTRAPPED = False


def _ensure_bootstrap_migrations(engine) -> None:
    global _MIGRATIONS_BOOTSTRAPPED
    if _MIGRATIONS_BOOTSTRAPPED:
        logger.info("[DB][MIGRATE][SKIP] already_bootstrapped=1")
        return
    run_migrations(engine)
    _MIGRATIONS_BOOTSTRAPPED = True
    logger.info("[DB][MIGRATE][BOOTSTRAP_DONE] once=1")


def _run_build_watchlist_job() -> int:
    """
    ✅ 설계 1: 주 1회 워치리스트 빌드 JOB.
    
    - 유니버스 로드
    - Minervini 필터 적용
    - week_monday(today) 키로 DB에 저장
    - 주문 없이 종료
    """
    logger.info("[WATCHLIST][BUILD_JOB] START")
    
    try:
        from datetime import date
        
        # DB 준비
        assert_db_ready()
        engine = make_engine()
        _ensure_bootstrap_migrations(engine)
        
        # 환경변수
        env = os.getenv("ENV", "live")
        strategy = os.getenv("STRATEGY", "best_k_meta")
        
        # 주간 키 계산
        today = now_kst().date()
        as_of = week_monday(today)
        
        logger.info("[WATCHLIST][BUILD_JOB] env=%s strategy=%s as_of=%s (today=%s)", env, strategy, as_of, today)
        
        # 유니버스 로드
        universe_repo = UniverseRepo(engine)
        universe_snapshot = universe_repo.get_current_universe_snapshot(env, strategy)
        
        if not universe_snapshot or not universe_snapshot.get("members"):
            logger.error("[WATCHLIST][BUILD_JOB][FAIL] universe empty")
            return 1
        
        members = universe_snapshot["members"]
        logger.info("[WATCHLIST][BUILD_JOB] universe loaded members=%s", len(members))
        
        # OHLCV provider 생성
        kis = KisAPI()
        krx_provider = KRXOHLCVProvider()
        kis_provider = KISOHLCVProvider(kis)
        # ✅ FIX: KIS 우선으로 변경 (pykrx JSONDecodeError 방지)
        ohlcv_provider = ChainOHLCVProvider([kis_provider, krx_provider])
        
        def _fetch_daily(code: str, count: int = 100):
            """pb1_engine._fetch_daily 호환 래퍼"""
            df = ohlcv_provider.fetch_daily(code, count=count)
            return df, "chain"
        
        # Minervini config
        minervini_config = MinerviniConfig()
        rs_min_pctile = float(getattr(minervini_config, "rs_min_percentile", getattr(minervini_config, "rs_min", 70.0)))
        if rs_min_pctile <= 1:
            rs_min_pctile *= 100
        heavy_volume_mult = float(getattr(minervini_config, "heavy_volume_mult", getattr(minervini_config, "heavy_vol_mult_10", 1.5)))
        time_stop_days = int(getattr(minervini_config, "time_stop_days", os.getenv("PB1_TIME_STOP_DAYS", 20)))
        minervini_config_dict = {
            # Keep both contracts while old watchlist builders are deployed.
            "rs_min": rs_min_pctile,
            "rs_min_percentile": rs_min_pctile,
            "breakout_vol_mult_20": minervini_config.breakout_vol_mult_20,
            "heavy_vol_mult_10": heavy_volume_mult,
            "heavy_volume_mult": heavy_volume_mult,
            "add_on_R": minervini_config.add_on_R,
            "max_pyramid_levels": minervini_config.max_pyramid_levels,
            "initial_stop_pct": minervini_config.initial_stop_pct,
            "time_stop_days": time_stop_days,
            "risk_pct_of_equity": minervini_config.risk_pct_of_equity,
            "add_on_size_frac": minervini_config.add_on_size_frac,
            "add_on_max_extension": minervini_config.add_on_max_extension,
        }
        
        # Watchlist 빌드 & 저장
        watchlist = build_and_save_watchlist(
            engine=engine,
            env=env,
            strategy=strategy,
            as_of=as_of,  # ✅ 주간 키
            members=members,
            ohlcv_provider=_fetch_daily,
            minervini_config=minervini_config_dict,
            force_rebuild=True,
        )
        
        logger.info("[WATCHLIST][BUILD_JOB] SUCCESS count=%s as_of=%s", len(watchlist), as_of)
        return 0
        
    except Exception as exc:
        logger.exception("[WATCHLIST][BUILD_JOB][FAIL] err=%s", exc)
        return 1


def _deepcopy_json(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _log_db_only_universe_precheck(
    *,
    repo: UniverseRepo,
    env: str,
    strategy: str,
    namespace: str = "default",
) -> None:
    snapshot = repo.get_current_universe_snapshot(env, strategy)
    if not snapshot:
        logger.warning(
            "[UNIVERSE][LOAD][DB_ONLY][MISS] source=db_only env=%s strategy=%s namespace=%s",
            env,
            strategy,
            namespace,
        )
        return
    logger.info(
        "[UNIVERSE][LOAD][DB_ONLY] source=db_only env=%s strategy=%s namespace=%s run_id=%s as_of=%s members=%s sample=%s",
        env,
        strategy,
        namespace,
        snapshot.get("run_id"),
        snapshot.get("as_of"),
        snapshot.get("members_count"),
        snapshot.get("sample_codes"),
    )


def generate_run_summary_json(
    *,
    run_id: str,
    trace_id: str,
    env: str,
    engine: object,
    as_of_requested: str | None = None,
    as_of_used: str | None = None,
    watchlist_as_of: str | None = None,
    universe_as_of: str | None = None,
    fallback_used: bool = False,
) -> str | None:
    """
    RUN 요약 JSON 생성 및 저장.
    
    파일: runtime/summary/run_{run_id}.json
    
    포함 내용:
    - run_id, trace, env, as_of 관련
    - counts: scanned/pb1_ok/minervini_ok/risk_ok/sizing_ok/orders
    - drop reason 집계
    """
    try:
        summary_root = runtime_path("runtime", "summary")
        summary_root.mkdir(parents=True, exist_ok=True)
        
        summary_path = summary_root / f"run_{run_id}.json"
        
        run_summary = getattr(engine, "_run_summary_payload", None) or {}
        debug_summary = getattr(engine, "_debug_summary", {}) or {}
        scanner_summary = getattr(engine, "_scanner_summary", {}) or {}
        exit_summary = getattr(engine, "_exit_summary_payload", {}) or {}
        prep_summary = getattr(engine, "_prep_summary_payload", {}) or {}
        reject_reason_counts = getattr(engine, "reject_reason_counts", {})
        counts = {
            "scanned": int(run_summary.get("scanned", debug_summary.get("scanned_count", 0))),
            "passed": int(run_summary.get("setup_ok", debug_summary.get("setup_ok_count", 0))),
            "setup_ok_count": int(run_summary.get("setup_ok", debug_summary.get("setup_ok_count", 0))),
            "after_relax_count": int(run_summary.get("relax_ok", debug_summary.get("after_relax_count", 0))),
            "after_score_cut_count": int(run_summary.get("score_ok", debug_summary.get("after_score_cut_count", 0))),
            "after_risk_count": int(run_summary.get("risk_ok", debug_summary.get("after_risk_count", 0))),
            "after_sizing_count": int(run_summary.get("sized_ok", debug_summary.get("after_sizing_count", 0))),
            "buyable_ok_count": int(run_summary.get("buyable_ok", debug_summary.get("after_buyable_count", 0))),
            "order_candidate_count": int(run_summary.get("order_candidates", debug_summary.get("order_candidate_count", 0))),
            "submit_attempt_count": int(debug_summary.get("submit_attempt_count", run_summary.get("order_candidates", 0))),
            "submit_success_count": int(run_summary.get("submitted", debug_summary.get("submit_success_count", 0))),
        }
        
        payload = {
            "run_id": str(run_id),
            "trace": str(trace_id),
            "env": str(env),
            "as_of_requested": as_of_requested,
            "as_of_used": as_of_used,
            "watchlist_as_of": watchlist_as_of,
            "universe_as_of": universe_as_of,
            "fallback_used": bool(fallback_used),
            "prep": prep_summary,
            "scanner": scanner_summary,
            "entry": {
                "scanned": counts["scanned"],
                "setup_ok": counts["setup_ok_count"],
                "score_ok": counts["after_score_cut_count"],
                "risk_ok": counts["after_risk_count"],
                "sized_ok": counts["after_sizing_count"],
                "buyable_ok": counts["buyable_ok_count"],
                "submitted": counts["submit_success_count"],
            },
            "exit": exit_summary,
            "counts": counts,
            "order_candidate_codes": list(debug_summary.get("order_candidate_codes", []) or []),
            "skip_reason_top": run_summary.get("no_trade_reason") or debug_summary.get("skip_reason_top", "none"),
            "blocked_reasons_counter": dict(run_summary.get("blocked_reasons_counter") or {}),
            "blocked_by": run_summary.get("blocked_by", "none"),
            "no_trade_reason": run_summary.get("no_trade_reason"),
            "entry_decision_result": run_summary.get("entry_decision_result"),
            "entry_decision_reason": run_summary.get("entry_decision_reason"),
            "drop_reasons": reject_reason_counts or {},
            "consistency": {
                "scanner_usable": int(scanner_summary.get("usable_count", scanner_summary.get("total", 0))),
                "raw_signal_setup_ok": int(scanner_summary.get("setup_ok_count", 0)),
                "pb1_filter_setup_ok": counts["setup_ok_count"],
                "risk_ok": counts["after_risk_count"],
                "sized_ok": counts["after_sizing_count"],
                "buyable_ok": counts["buyable_ok_count"],
                "match": int(int(scanner_summary.get("setup_ok_count", 0)) == counts["setup_ok_count"]),
            },
        }
        
        summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(
            "[RUN_SUMMARY] path=%s run_id=%s scanned=%s passed=%s drop_reasons=%s",
            summary_path,
            run_id,
            counts["scanned"],
            counts["passed"],
            len(reject_reason_counts),
        )
        logger.info(
            "[RUN_SUMMARY][ENTRY] scanned=%s setup_ok=%s relax_ok=%s score_ok=%s risk_ok=%s sized_ok=%s order_candidates=%s submitted=%s",
            counts["scanned"],
            counts["setup_ok_count"],
            counts["after_relax_count"],
            counts["after_score_cut_count"],
            counts["after_risk_count"],
            counts["after_sizing_count"],
            counts["order_candidate_count"],
            counts["submit_success_count"],
        )
        return str(summary_path)
    except Exception as e:
        logger.warning(
            "[RUN_SUMMARY][FAIL] run_id=%s error=%s",
            run_id,
            type(e).__name__,
            exc_info=False,
        )
        return None


def get_balance_state(
    *,
    kis: KisAPI,
    now: datetime,
) -> tuple[str, dict | None, str | None]:
    try:
        balance_snapshot_raw, balance_source, raw_snapshot = kis.get_balance_cached(
            force=False,
            return_source=True,
            return_raw=True,
        )
        balance_snapshot_raw = _deepcopy_json(balance_snapshot_raw)
        raw_snapshot = _deepcopy_json(raw_snapshot)
        if not isinstance(balance_snapshot_raw, dict):
            raise KisBalanceUnavailable("balance_snapshot_not_dict")
        if not isinstance(balance_snapshot_raw.get("output1"), list):
            raise KisBalanceUnavailable("balance_output1_not_list")
        if not isinstance(balance_snapshot_raw.get("output2"), dict):
            raise KisBalanceUnavailable("balance_output2_not_dict")
        return BALANCE_STATE_OK, balance_snapshot_raw, balance_source
    except KisBalanceUnavailable as exc:
        logger.warning("[PB1][BALANCE][UNAVAILABLE] %s", exc)
    except Exception:
        logger.exception("[PB1][BALANCE][FAIL] initial snapshot")
    return BALANCE_STATE_UNKNOWN, None, None


def resolve_kr_balance_precheck_for_test(path: Path) -> tuple[str, dict | None, str | None]:
    """Small testable adapter for KR session-level balance precheck restoration."""
    pre = json.loads(Path(path).read_text(encoding="utf-8"))
    state = str(pre.get("state") or "UNKNOWN").upper()
    snapshot = pre.get("raw_snapshot") if isinstance(pre.get("raw_snapshot"), dict) else None
    source = str(pre.get("source") or "NONE")
    if state == "OK" and snapshot:
        logger.info("[PB1][BALANCE_PRECHECK_USE] state=OK source=%s requery=0 snapshot=1", source)
        return BALANCE_STATE_OK, snapshot, source
    if state == "OK":
        logger.info("[PB1][BALANCE_PRECHECK_USE] state=UNKNOWN_WITHOUT_SNAPSHOT source=%s requery=0 snapshot=0", source)
        return BALANCE_STATE_UNKNOWN, None, source
    logger.info("[PB1][BALANCE_PRECHECK_USE] state=%s source=%s requery=0 snapshot=0", state, source)
    return BALANCE_STATE_UNKNOWN, None, source


def _diag_balance_probe_once_safe(*, logger, runtime_root_dir: Path, kis_factory):
    """
    DIAG에서 잔고/예수금/주문가능을 1회만 조회하고, flag 파일로 중복 실행 방지.
    - 전역변수/스코프 의존 금지
    - 런타임 경로를 하드코딩하지 않고 runtime_root 사용
    """
    import os
    import json
    import time

    diag_dir = runtime_root_dir / "runtime" / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    flag_path = diag_dir / "diag_balance_once.flag"
    if flag_path.exists():
        logger.info("[DIAG][BALANCE] already probed -> skip flag=%s", flag_path)
        return

    try:
        with flag_path.open("w", encoding="utf-8") as f:
            f.write(str(int(time.time())))
    except Exception as e:
        logger.warning("[DIAG][BALANCE] failed to write flag: %s", e)

    kis = kis_factory()
    raw = kis.get_balance(force=True)

    out2 = None
    try:
        out2 = (raw or {}).get("output2")
        if isinstance(out2, list) and out2:
            out2 = out2[0]
    except Exception:
        out2 = None

    def _to_int(x):
        try:
            if x is None:
                return None
            s = str(x).strip().replace(",", "")
            if s == "":
                return None
            return int(float(s))
        except Exception:
            return None

    def _pick(d, keys):
        if not isinstance(d, dict):
            return None
        for k in keys:
            if k in d:
                v = _to_int(d.get(k))
                if v is not None:
                    return (k, v)
        return None

    candidates_total = [
        "tot_evlu_amt", "evlu_amt_smtl", "tot_asst_amt", "tot_asset_amt",
        "tot_asst_amt2", "tot_evlu_amt2"
    ]
    candidates_orderable = [
        "ord_psbl_cash", "ord_psbl_amt", "nxdy_excc_amt", "dnca_tot_amt"
    ]
    candidates_deposit = [
        "dnca_tot_amt", "prvs_rcdl_excc_amt", "nxdy_excc_amt", "cma_evlu_amt"
    ]

    picked_total = _pick(out2, candidates_total)
    picked_orderable = _pick(out2, candidates_orderable)
    picked_deposit = _pick(out2, candidates_deposit)

    logger.info("[DIAG][BALANCE][RAW_KEYS] output2_keys=%s",
                sorted(list(out2.keys())) if isinstance(out2, dict) else None)
    logger.info("[DIAG][BALANCE][PICK] total=%s orderable=%s deposit=%s",
                picked_total, picked_orderable, picked_deposit)

    raw_path = os.path.join(diag_dir, "diag_balance_raw.json")
    try:
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False)
        logger.info("[DIAG][BALANCE] saved raw=%s", raw_path)
    except Exception as e:
        logger.warning("[DIAG][BALANCE] failed to save raw json: %s", e)


def ensure_universe_built_once(
    *,
    engine,
    env: str | None = None,
    strategy: str | None = None,
    as_of: str | None = None,
) -> list[dict]:
    env = env or os.getenv("KIS_ENV") or os.getenv("ENV")
    if env is not None:
        env = env.strip()
    strategy = strategy or os.getenv("STRATEGY_NAME", DEFAULT_UNIVERSE_STRATEGY)
    as_of = as_of or _get_now_kst().date().isoformat()

    assert env, "env is required"
    assert strategy, "strategy is required"
    assert as_of, "as_of is required"

    log.info("[UNIVERSE][ENSURE][DB] env=%s strategy=%s as_of=%s", env, strategy, as_of)
    repo = UniverseRepo(engine)
    
    # Step A) 오늘 유니버스 DB 로드
    members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of)
    if members:
        return members

    # Step B) 없으면(빈 리스트) 오늘 유니버스 build & persist 시도
    try:
        from trader.universe import build as universe_build
        built = universe_build.build_universe(as_of_date=as_of, env=env, strategy=strategy)
        if built:
            repo.save_universe_run_and_members(env=env, strategy=strategy, as_of=as_of, members=built)
            members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of)
            if members:
                return members
    except Exception as exc:
        logger.warning("[UNIVERSE][BUILD][FAIL] env=%s strategy=%s as_of=%s err=%s", env, strategy, as_of, exc)

    # Step C) build 실패 또는 DB 오류면 “최근 유니버스 fallback”
    allow_fallback = os.getenv("ALLOW_UNIVERSE_DB_FAIL", "0") == "1"
    if allow_fallback:
        latest = repo.get_latest_universe_members(env=env, strategy=strategy, as_of_date=as_of)
        if latest:
            logger.warning("[UNIVERSE][FALLBACK] using latest as_of<=%s count=%d", as_of, len(latest))
            return latest

    # Step D) fallback도 없으면 “이번 루프 스킵(거래 안 함) + 다음 루프에서 재시도”
    logger.error("[UNIVERSE][EMPTY] no universe available. skip this cycle and retry next loop.")
    return []



def _norm_env(x: str | None) -> str:
    """Normalize env keys to avoid 'paper' vs 'PAPER' DB misses."""
    if not x:
        return ""
    return str(x).strip().lower()


def _safe_load_universe_snapshot(repo, *, env: str, strategy: str, as_of):
    """
    Backward/forward compatible universe loader.
    - Newer code may have UniverseRepo.get_latest_successful_universe_snapshot
    - Older repos may only have get_current_universe_members / get_latest_watchlist_date style APIs
    This function prevents AttributeError and normalizes env casing.
    """
    env = _norm_env(env)
    strategy = str(strategy).strip()

    # 1) Preferred API (if exists)
    if hasattr(repo, "get_latest_successful_universe_snapshot"):
        return repo.get_latest_successful_universe_snapshot(env=env, strategy=strategy, as_of_date=as_of)

    # 2) Common older API: get_current_universe_members
    if hasattr(repo, "get_current_universe_members"):
        try:
            # some versions accept as_of / as_of_date
            return {
                "as_of": as_of,
                "env": env,
                "strategy": strategy,
                "members": repo.get_current_universe_members(env=env, strategy=strategy, as_of=as_of) or [],
            }
        except TypeError:
            # older signature: (env, strategy) only
            return {
                "as_of": as_of,
                "env": env,
                "strategy": strategy,
                "members": repo.get_current_universe_members(env=env, strategy=strategy) or [],
            }

    # 3) Last resort: try load methods (names vary)
    for name in ("load_universe", "load_universe_members", "get_universe_members"):
        if hasattr(repo, name):
            fn = getattr(repo, name)
            try:
                members = fn(env=env, strategy=strategy, as_of=as_of) or []
            except TypeError:
                try:
                    members = fn(env=env, strategy=strategy) or []
                except TypeError:
                    members = fn(as_of=as_of) or []
            return {"as_of": as_of, "env": env, "strategy": strategy, "members": members}

    # If we reach here, repo API is unknown
    return {"as_of": as_of, "env": env, "strategy": strategy, "members": []}


def _load_universe_context(
    *,
    engine,
    as_of: str,
    env: str,
    strategy: str,
) -> UniverseContext:
    # [2026-06-01] PB1_EXIT_ONLY_MODE: PREP 실패 시 final30 없이 KIS balance 기반으로 보유종목 exit 관리
    if os.getenv("PB1_EXIT_ONLY_MODE", "0") == "1":
        logger.info(
            "[PB1][EXIT_ONLY_MODE] bypassing final30 load env=%s as_of=%s -> empty context, entry blocked",
            (env or "").strip().lower(),
            as_of,
        )
        os.environ["PB1_ENTRY_ENABLED"] = "0"
        return UniverseContext(
            as_of_date=str(as_of),
            members=[],
            selected_path=None,
            meta={
                "source": "exit_only_kis_balance",
                "exit_only": True,
                "locked": False,
                "is_scored": False,
                "usable": True,
            },
            is_empty=True,
        )

    mode_input = (os.getenv("MODE") or "").strip().lower()
    if mode_input == "trade" or os.getenv("PB1_TRADE_WATCHLIST_ONLY", "0") == "1":
        requested_strategy = (strategy or "").strip() or os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
        if not os.getenv("PB1_UNIVERSE_STRATEGY"):
            os.environ["PB1_UNIVERSE_STRATEGY"] = "pb1_watchlist_final_scored"
        trade_strategy = (os.getenv("PB1_UNIVERSE_STRATEGY") or requested_strategy or "pb1_watchlist_final_scored").strip()
        logger.info(
            "[TRADE][FINAL30][LOAD_REQUEST] env=%s as_of=%s strategy=%s mode=trade",
            (env or "").strip().lower(),
            as_of,
            trade_strategy,
        )
        if trade_strategy != "pb1_watchlist_final_scored":
            logger.error(
                "[TRADE][FINAL30][LOCK][FAIL] reason=source_not_scored actual_source=%s",
                trade_strategy,
            )
            _raise_entry_abort_precheck("db_exact_scored_not_loaded")
        result = load_locked_final30_from_db(engine=engine, env=env, derived_as_of=as_of)
        df = result.get("df")
        df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        rows = len(df)
        result_source = str(result.get("source_name") or "none")
        result_columns = [str(col) for col in df.columns.tolist()]
        missing_scored = _missing_scored_cols(result_columns)
        logger.info(
            "[TRADE][FINAL30][LOAD_RESULT] source=%s rows=%s is_scored=%s cols=%s",
            result_source,
            rows,
            int(bool(result.get("is_scored"))),
            result_columns,
        )
        _validate_trade_locked_final30_or_raise(final30_df=df, source_name=result_source, as_of=str(as_of), env=str(env))

        require_scored = os.getenv("TRADE_REQUIRE_PREP_FINAL30_SCORED", "1") == "1"
        if require_scored and not bool(result.get("is_scored")):
            logger.error(
                "[TRADE][FINAL30][LOCK][FAIL] reason=missing_scored_cols missing=%s",
                missing_scored,
            )
            _raise_entry_abort_precheck("db_exact_scored_missing_critical_cols")

        if "rank_final30" in df.columns:
            df = df.sort_values(by=["rank_final30", "code"], ascending=[True, True], kind="mergesort")
        elif "score_final" in df.columns:
            df = df.sort_values(by=["score_final", "code"], ascending=[False, True], kind="mergesort")
        else:
            df = df.sort_values(by=["code"], ascending=[True], kind="mergesort")

        rank_basis = "rank_final30" if "rank_final30" in df.columns else ("score_final" if "score_final" in df.columns else "code")
        original_codes = [str(x).zfill(6) for x in pd.DataFrame(result.get("df")).get("code", pd.Series(dtype=str)).tolist() if str(x).strip()]
        sorted_codes = [str(x).zfill(6) for x in df.get("code", pd.Series(dtype=str)).tolist() if str(x).strip()]
        canonical_order_match = int(original_codes == sorted_codes)
        locked_final30_rows = [dict(row or {}) for row in df.to_dict(orient="records")]

        members = [dict(row) for row in df.to_dict(orient="records") if row.get("code")]
        for member in members:
            member["code"] = str(member.get("code") or "").zfill(6)

        top10_codes = [m.get("code") for m in members[:10] if m.get("code")]
        logger.info(
            "[TRADE][FINAL30][DB_LOCK] env=%s as_of=%s rows=%s scored=%s immutable=1",
            (env or "").strip().lower(),
            result.get("as_of"),
            len(members),
            int(bool(result.get("is_scored"))),
        )
        logger.info(
            "[TRADE][FINAL30][ORDER] rank_basis=%s canonical_order_match=%s",
            rank_basis,
            canonical_order_match,
        )
        logger.info(
            "[TRADE][FINAL30][TOP10] source=%s rank_basis=%s codes=%s",
            result.get("source_name"),
            rank_basis,
            top10_codes,
        )
        return UniverseContext(
            as_of_date=str(result.get("as_of") or as_of),
            members=members,
            selected_path=None,
            meta={
                "source": result.get("source_name"),
                "as_of": str(result.get("as_of") or as_of),
                "locked": True,
                "is_scored": bool(result.get("is_scored")),
                "usable": bool(result.get("usable")),
                "columns": list(result.get("columns") or []),
                "missing_scored_cols": list(result.get("missing_scored_cols") or []),
                "flow_optional_missing": list(result.get("flow_optional_missing") or []),
                "locked_final30_rows": locked_final30_rows,
                "locked_final30_is_scored": True,
                "path_map": dict(result.get("path_map") or {}),
                "source_compare": dict(result.get("source_compare") or {}),
                "used_fallback": bool(result.get("used_fallback")),
                "file_mirror_present": int(bool(result.get("file_mirror_present"))),
            },
            is_empty=len(members) == 0,
        )

    # [NEW] WATCHLIST_MODE=1이면 WATCHLIST env에서 직접 로딩
    watchlist_mode = os.getenv("WATCHLIST_MODE", "0") == "1"
    if watchlist_mode:
        watchlist_codes = os.getenv("WATCHLIST", "005930,000660,035420,035720,005380")
        codes = [c.strip() for c in watchlist_codes.split(",") if c.strip()]
        members = [{"code": code, "name": code} for code in codes]
        logger.info(
            "[PB1][WATCHLIST_MODE] bypassing universe -> using %d codes: %s",
            len(codes),
            codes[:10],
        )
        return UniverseContext(
            as_of_date=as_of,
            members=members,
            selected_path=None,
            meta={"source": "watchlist_env", "codes": codes},
            is_empty=False,
        )
    
    repo = UniverseRepo(engine)
    env = _norm_env(env)
    members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of)
    universe_actual_as_of = as_of  # 기본값 (fallback 감지 필요)
    
    if not members and is_db_only_mode():
        fallback = repo.get_current_universe_snapshot(env, strategy)
        if fallback and fallback.get("members"):
            universe_actual_as_of = str(fallback.get("as_of") or as_of)
            logger.warning(
                "[PB1][UNIVERSE][DB_ONLY] fallback_current run_id=%s requested_as_of=%s actual_as_of=%s members=%s",
                fallback.get("run_id"),
                as_of,
                universe_actual_as_of,
                fallback.get("members_count"),
            )
            return UniverseContext(
                as_of_date=universe_actual_as_of,
                members=fallback.get("members") or [],
                selected_path=None,
                meta={"source": "db_only", "requested_as_of": as_of, "actual_as_of": universe_actual_as_of},
                is_empty=False,
            )
        env = _norm_env(env)
        latest = _safe_load_universe_snapshot(repo, env=env, strategy=strategy, as_of=as_of)
        members = (latest or {}).get("members") or []
        if latest and latest.get("members"):
            universe_actual_as_of = str(latest.get("as_of") or as_of)
            logger.warning(
                "[PB1][UNIVERSE][DB_ONLY] fallback_latest run_id=%s requested_as_of=%s actual_as_of=%s members=%s",
                latest.get("run_id"),
                as_of,
                universe_actual_as_of,
                latest.get("members_count"),
            )
            return UniverseContext(
                as_of_date=universe_actual_as_of,
                members=latest.get("members") or [],
                selected_path=None,
                meta={"source": "db_only", "requested_as_of": as_of, "actual_as_of": universe_actual_as_of},
                is_empty=False,
            )
        logger.error("[PB1][UNIVERSE][DB_ONLY][MISS] env=%s strategy=%s as_of=%s", env, strategy, as_of)
        raise RuntimeError("db_only_universe_missing")
    if not members:
        logger.warning(
            "[PB1][UNIVERSE][EMPTY_OK] requested_as_of=%s -> skip trading (오늘은 조건 맞는 종목 없음(미너비니 필터 0))",
            as_of,
        )
        return UniverseContext(
            as_of_date=as_of,
            members=[],
            selected_path=None,
            meta={"source": "db", "requested_as_of": as_of, "actual_as_of": as_of},
            is_empty=True,
        )
    return UniverseContext(
        as_of_date=as_of,
        members=members,
        selected_path=None,
        meta={"source": "db", "requested_as_of": as_of, "actual_as_of": as_of},
        is_empty=False,
    )


def _resolve_market_context(
    *,
    now: datetime,
    trading_day: bool,
    market_window: str,
    window_override: str,
    phase_seed: str | None,
) -> tuple[WindowDecision | None, str, str, str, str, list[str]]:
    window = decide_window(now=now, override=window_override)
    window_label = _resolve_window_label(market_window, window)
    force_entry_window_override = _env_flag("PB1_FORCE_ENTRY_WINDOW_OVERRIDE", default=False)
    forced_trade_session = str(os.getenv("PB1_FORCED_TRADE_SESSION") or "").strip().lower()
    resolved_phase, phase_reason, phase_window = resolve_pb1_phase(
        now,
        trading_day,
        phase_seed,
        force_entry_window_override=force_entry_window_override,
        forced_trade_session=forced_trade_session,
    )
    reasons: list[str] = []
    if not resolved_phase:
        fallback = (os.getenv("PB1_PHASE_DEFAULT") or "entry").strip().lower()
        resolved_phase = fallback if fallback else "entry"
        phase_reason = "default_env"
        reasons.append("phase_default_env")
    if force_entry_window_override and forced_trade_session in {"am", "pm"}:
        if window_label not in {"preopen", "morning", "day"}:
            window_label = "day"
            reasons.append("trade_session_window_override")
        if resolved_phase != "entry":
            resolved_phase = "entry"
        phase_reason = "force_entry_window_override"
        reasons.append("trade_session_phase_override")
    reasons.append(f"market_window:{market_window}")
    reasons.append(f"window:{window_label}")
    reasons.append(f"phase:{resolved_phase}")
    return window, window_label, resolved_phase, phase_reason, phase_window, reasons


def _resolve_window_label(market_window: str, window: WindowDecision | None) -> str:
    # ✅ DIAG_FULL_EXEC이면 window 덮어쓰기 방지
    diag_full_exec = env_bool("PB1_DIAG_FULL_EXEC", False)
    strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
    
    normalized = (market_window or "").strip().lower()
    if normalized in {"morning", "day", "close", "preopen", "after", "afternoon", "off"}:
        if window and window.name and window.name != normalized:
            # DIAG_FULL_EXEC이면 경고 없이 normalized 사용
            if diag_full_exec and strategy_mode == "DIAG":
                return normalized
            
            global _WINDOW_MISMATCH_LOGGED
            if not _WINDOW_MISMATCH_LOGGED:
                logger.warning(
                    "[PB1][WINDOW][WARN] market_window=%s mismatch window=%s -> forcing %s",
                    normalized,
                    window.name,
                    normalized,
                )
                _WINDOW_MISMATCH_LOGGED = True
        return normalized
    return window.name if window else "none"


def normalize_window(*, session_kind: str, input_window: Any) -> str:
    normalized_session = str(session_kind or "").strip().lower()
    normalized_input = str(input_window or "").strip().lower()
    
    # CRITICAL: close window must stay close
    if normalized_input == "close":
        logger.info(
            "[PB1][WINDOW][NORMALIZE] session_kind=%s input_window=close normalized_window=close",
            normalized_session,
        )
        return "close"
    
    if normalized_session == "am" and normalized_input in {"am", "morning", "intraday", "day", "preopen", "session", "open"}:
        logger.info(
            "[PB1][WINDOW][NORMALIZE] session_kind=%s input_window=%s normalized_window=morning",
            normalized_session,
            normalized_input or "none",
        )
        return "morning"
    if normalized_input in {"preopen", "morning", "intraday"}:
        return "intraday"
    if normalized_input == "after":
        return "after"
    return "day"


def _normalize_window_phase(*, raw_window: Any, market_window: str, phase: str) -> tuple[str, str]:
    raw_name = ""
    raw_type = type(raw_window).__name__ if raw_window is not None else "NoneType"
    if isinstance(raw_window, str):
        raw_name = raw_window
    elif raw_window is not None:
        raw_name = str(getattr(raw_window, "name", "") or "")
    seed = (market_window or raw_name or "day").strip().lower()
    session_kind = _resolve_session_kind()
    window_name = normalize_window(session_kind=session_kind, input_window=seed)
    phase_seed = (phase or "entry").strip().lower()
    if phase_seed in {"entry", "pm_entry"}:
        phase_name = phase_seed
    elif phase_seed in {"exit", "close"}:
        phase_name = "exit"
    elif phase_seed == "manage":
        phase_name = phase_seed
    else:
        phase_name = "entry"
    logger.info(
        "[WINDOW][NORMALIZED] raw_type=%s raw_name=%s normalized_window=%s normalized_phase=%s",
        raw_type,
        raw_name or "none",
        window_name,
        phase_name,
    )
    return window_name, phase_name


def _parse_hhmm_to_time(hhmm: str) -> dtime:
    hh, mm = hhmm.split(":")
    return dtime(hour=int(hh), minute=int(mm))


def _next_window_start(now: datetime, window_starts: list[dtime]) -> datetime | None:
    sorted_starts = sorted(window_starts)
    for start in sorted_starts:
        if now.time() < start:
            return now.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
    return None


def _market_session(now: datetime) -> tuple[datetime, datetime]:
    open_t = _parse_hhmm_to_time(MARKET_OPEN_HHMM)
    close_t = _parse_hhmm_to_time(MARKET_CLOSE_HHMM)
    return (
        now.replace(hour=open_t.hour, minute=open_t.minute, second=0, microsecond=0),
        now.replace(hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0),
    )


def _parse_now_override(raw: str | None, label: str) -> datetime | None:
    if not raw:
        return None
    engine_runner: PB1Engine | None = None
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        else:
            dt = dt.astimezone(ZoneInfo("Asia/Seoul"))
        return dt
    except Exception:
        logger.warning("[PB1][SMOKE] invalid %s=%s", label, raw)
        return None


def _get_now_kst() -> datetime:
    override = _parse_now_override(os.getenv("NOW_KST_OVERRIDE"), "NOW_KST_OVERRIDE")
    if override:
        return override
    simulated = _parse_now_override(os.getenv("PB1_SIMULATE_NOW_KST"), "PB1_SIMULATE_NOW_KST")
    if simulated:
        return simulated
    return now_kst()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _env_bool_any(names: tuple[str, ...], default: bool = False) -> bool:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        return parse_bool_any(raw, default=default)
    return default


def _compute_only_full_run_flags(*, dry_run: bool | None = None) -> dict[str, bool]:
    dry_run_flag = dry_run if dry_run is not None else _env_bool_any(("DRY_RUN", "DRYRUN"), default=True)
    force_block_live = _env_bool_any(("FORCE_BLOCK_LIVE",), default=False)
    disable_live_trading = _env_bool_any(("DISABLE_LIVE_TRADING",), default=False)
    force_compute = _env_bool_any(("FORCE_COMPUTE_ON_CUTOFF", "FORCE_COMPUTE_WHEN_CUTOFF"), default=False)
    bypass_cutoff = _env_bool_any(("BYPASS_ENTRY_CUTOFF_COMPUTE_ONLY", "BYPASS_ENTRY_CUTOFF_FOR_COMPUTE"), default=False)

    enabled = bool(dry_run_flag or force_block_live or disable_live_trading) and bool(force_compute and bypass_cutoff)
    return {
        "enabled": enabled,
        "dry_run": bool(dry_run_flag),
        "force_block_live": bool(force_block_live),
        "disable_live_trading": bool(disable_live_trading),
        "force_compute": bool(force_compute),
        "bypass_cutoff": bool(bypass_cutoff),
    }


def decide_market_window(now: datetime) -> str:
    forced_override = (os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
    if forced_override in {"preopen", "morning", "day", "close", "after"}:
        return forced_override

    alias_override = (os.getenv("PB1_WINDOW_OVERRIDE") or "").strip().lower()
    if alias_override in {"preopen", "morning", "day", "close", "after"}:
        return alias_override

    trading_day_env = os.getenv("TRADING_DAY")
    if trading_day_env is not None:
        trading_day = _env_bool("TRADING_DAY", default=True)
        if not trading_day:
            return "after"

    if not is_trading_weekday(now):
        return "after"

    computed = calc_market_window_kst(now)
    forced = (os.getenv("MARKET_WINDOW") or "").strip().lower()
    if forced in {"preopen", "morning", "day", "close", "after"}:
        return forced

    return computed


def detect_window(now_kst_value: datetime, preopen_start: str = "08:45", preopen_end: str = "09:00") -> str:
    return decide_market_window(now_kst_value)


def _decide_action(now: datetime, trading_day: bool, open_dt: datetime, close_dt: datetime, allow_wait: bool, max_wait_s: int, smoke_enabled: bool) -> tuple[str, datetime | None]:
    force_run = env_bool("FORCE_RUN", default=False)
    forced_window = (os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
    forced_phase = (os.getenv("FORCE_PB1_PHASE") or "").strip().lower()
    if force_run and forced_window == "close" and forced_phase == "exit":
        return "run", None
    if smoke_enabled:
        return "smoke", None
    if not trading_day:
        return "smoke", None
    if now < open_dt:
        remaining = (open_dt - now).total_seconds()
        if allow_wait and remaining <= max_wait_s:
            return "wait", open_dt
        return "smoke", open_dt
    if now >= close_dt:
        return "smoke", None
    return "run", None


def _log_balance_cache(force: bool) -> None:
    logger.info("[RUNNER][BALANCE_CACHE] hit=%s", not force)


def _run_smoke(engine, kis_env: str, now: datetime) -> None:
    token_ok = balance_ok = universe_ok = pretrade_ok = False
    kis: KisAPI | None = None
    members: list[dict] = []
    try:
        kis = KisAPI()
        token_ok = True
    except Exception as exc:
        logger.warning("[SMOKE][FAIL] token_init err=%s", exc)

    if kis:
        try:
            _log_balance_cache(force=True)
            snap = kis.get_balance_cached(force=True)
            balance_ok = bool(snap)
        except Exception:
            logger.exception("[SMOKE][FAIL] balance")

    repo = UniverseRepo(engine)
    try:
        members = repo.get_current_universe_members(kis_env, "best_k_meta")
        if not members:
            if os.getenv("ALLOW_UNIVERSE_BUILD_IN_TRADE", "0") == "1" and not is_db_only_mode():
                from trader.universe import build as universe_build

                # ✅ CRITICAL: smoke test도 derived_as_of 사용
                smoke_derived_as_of_date = resolve_derived_as_of(now)
                smoke_as_of = smoke_derived_as_of_date.isoformat()
                
                logger.info(
                    "[ASOF][SMOKE][UNIVERSE] trade_date=%s derived_as_of=%s",
                    now.date().isoformat(),
                    smoke_as_of,
                )
                
                universe_build.build_universe(as_of_date=smoke_as_of, env=kis_env, strategy="best_k_meta")
                members = repo.get_current_universe_members(kis_env, "best_k_meta")
            else:
                logger.info("[UNIVERSE][SKIP] forbidden during trade path")
        universe_ok = bool(members)
    except Exception:
        logger.exception("[SMOKE][FAIL] universe")

    if kis:
        try:
            code = members[0]["code"] if members else "005930"
            quote = kis.get_price_quote(code, diag_mode=True, attempts=1)
            pretrade_ok = bool(quote)
        except Exception:
            logger.exception("[SMOKE][FAIL] pretrade")

    status = token_ok and balance_ok and universe_ok and pretrade_ok
    if status:
        logger.info(
            "[SMOKE][PASS] token=%s balance=%s universe=%s pretrade=%s",
            token_ok,
            balance_ok,
            universe_ok,
            pretrade_ok,
        )
    else:
        logger.warning(
            "[SMOKE][FAIL] token=%s balance=%s universe=%s pretrade=%s",
            token_ok,
            balance_ok,
            universe_ok,
            pretrade_ok,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PB1 close pullback runner")
    parser.add_argument(
        "--window",
        default="auto",
        choices=["auto", "preopen", "morning", "day", "close"],
        help="Execution window override",
    )
    parser.add_argument("--phase", default="auto", choices=["auto", "entry", "exit", "verify"], help="Phase override")
    parser.add_argument("--env", type=str, default=None, help="DB/KIS environment namespace")
    return parser.parse_args()


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("[PB1][ENV] invalid %s=%s fallback=%s", name, raw, default)
        return default


def _parse_optional_int_env(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    raw = raw.strip()
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("[PB1][ENV] invalid %s=%s fallback=none", name, raw)
        return None


def _resolve_loop_limits() -> tuple[int, int, int, bool]:
    """
    루프 제한 시간 계산.
    
    Returns:
        (run_loop_minutes, max_minutes, max_seconds, run_loop_configured)
    
    정책:
        - PB1_LOOP_MODE=UNTIL_CLOSE: 장 마감까지 루프 (max_minutes=0으로 무제한)
        - 기존 PB1_RUN_LOOP_MINUTES/PB1_MAX_MINUTES 설정 유지
    """
    loop_mode = os.getenv("PB1_LOOP_MODE", "").upper()
    
    # UNTIL_CLOSE 모드: 장 마감까지 무제한
    if loop_mode == "UNTIL_CLOSE":
        logger.info("[LOOP_LIMITS] mode=UNTIL_CLOSE -> max_minutes=0 (unlimited until close)")
        return 0, 0, 0, True
    
    # 기존 로직 (하위 호환)
    run_loop_minutes_env = _parse_optional_int_env("PB1_RUN_LOOP_MINUTES")
    if run_loop_minutes_env is None:
        run_loop_minutes_env = _parse_optional_int_env("RUN_LOOP_MINUTES")
    run_loop_configured = run_loop_minutes_env is not None
    run_loop_minutes = run_loop_minutes_env if run_loop_minutes_env is not None else 15

    max_minutes_env = _parse_optional_int_env("PB1_MAX_MINUTES")
    max_minutes = max_minutes_env if max_minutes_env is not None else run_loop_minutes

    max_seconds_env = _parse_optional_int_env("PB1_MAX_SECONDS")
    max_seconds = max_seconds_env if max_seconds_env is not None else max(0, max_minutes * 60)

    return run_loop_minutes, max_minutes, max_seconds, run_loop_configured


def _change_flag_path() -> Path:
    cache_root = Path(os.getenv("TRADER_CACHE_ROOT") or runtime_root() / "runtime")
    return cache_root / "changed.flag"


def _write_change_flag(changed: bool, reasons: list[str]) -> None:
    flag_path = _change_flag_path()
    flag_path.parent.mkdir(parents=True, exist_ok=True)
    flag_path.write_text("1\n" if changed else "0\n", encoding="utf-8")
    logger.info("[PB1][CHANGE] changed=%s reasons=%s", int(changed), reasons)


def _write_last_db_write(runtime_root_dir: Path, *, run_id: str | None, reason: str, now: datetime) -> Path:
    payload = {
        "run_id": run_id,
        "reason": reason,
        "ts": now.isoformat(),
    }
    target = runtime_root_dir / "runtime" / "status" / "last_db_write.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def _extract_dnca_total(balance_snapshot: dict | None) -> int | None:
    if not balance_snapshot:
        return None
    summary_raw = balance_snapshot.get("output2")
    summary = summary_raw[0] if isinstance(summary_raw, list) and summary_raw else summary_raw if isinstance(summary_raw, dict) else None
    if not isinstance(summary, dict):
        return None
    raw = summary.get("dnca_tot_amt")
    if raw is None:
        return None
    try:
        return int(float(str(raw).replace(",", "")))
    except Exception:
        return None


def _extract_kis_holdings_count(balance_snapshot: dict | None) -> int | None:
    if not isinstance(balance_snapshot, dict):
        return None
    rows = balance_snapshot.get("output1")
    if not isinstance(rows, list):
        return None
    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        qty_raw = row.get("hldg_qty") or row.get("qty") or row.get("ord_psbl_qty") or 0
        try:
            qty = int(float(str(qty_raw).replace(",", "")))
        except Exception:
            qty = 0
        if qty > 0:
            count += 1
    return count


def _resolve_practice_account_key(kis: KisAPI | None, env: str) -> str:
    return get_account_key(env=env, kis=kis)


def _practice_account_sanity_check(
    *,
    env: str,
    kis: KisAPI | None,
    balance_state: str,
    balance_snapshot: dict | None,
) -> dict[str, Any]:
    env_name = str(env or "").strip().lower()
    if env_name != "practice":
        return {"enabled": False, "ok": True, "reason": "non_practice"}
    if not account_env_flag("ACCOUNT_SANITY_CHECK", default=False):
        return {"enabled": False, "ok": True, "reason": "disabled"}

    expected_capital = expected_practice_capital_krw()
    expected_holdings_count = expected_initial_holdings()
    capital_tolerance = resolve_account_sanity_capital_tolerance_krw(expected_capital)
    account_key = get_account_key(env=env_name, kis=kis)
    masked_account = get_masked_account_key(env=env_name, kis=kis)
    holdings_count = _extract_kis_holdings_count(balance_snapshot)
    cash_krw = _extract_dnca_total(balance_snapshot)
    reasons: list[str] = []

    if balance_state == BALANCE_STATE_UNKNOWN:
        reasons.append("balance_unknown")
    if holdings_count is None:
        reasons.append("holdings_unknown")
    elif expected_holdings_count >= 0 and holdings_count != expected_holdings_count:
        reasons.append("holdings_mismatch")
    if cash_krw is None:
        reasons.append("capital_unknown")
    elif expected_capital > 0 and abs(cash_krw - expected_capital) > capital_tolerance:
        reasons.append("capital_mismatch")

    return {
        "enabled": True,
        "ok": not reasons,
        "reason": ",".join(reasons) if reasons else "ok",
        "account_key": account_key,
        "masked_account": masked_account,
        "expected_capital_krw": expected_capital,
        "expected_holdings": expected_holdings_count,
        "capital_tolerance_krw": capital_tolerance,
        "cash_krw": cash_krw,
        "holdings_count": holdings_count,
        "balance_state": balance_state,
    }


def _maybe_reconcile_practice_account_state(
    *,
    engine,
    env: str,
    kis: KisAPI | None,
    balance_state: str,
    balance_snapshot: dict | None,
    positions_repo: PositionsRepo,
) -> dict[str, Any] | None:
    env_name = str(env or "").strip().lower()
    if env_name != "practice":
        return None
    if balance_state == BALANCE_STATE_UNKNOWN:
        logger.warning("[ACCOUNT_RECONCILE][SKIP] reason=kis_holdings_unavailable env=%s", env_name)
        return None
    kis_holdings_count = _extract_kis_holdings_count(balance_snapshot)
    if kis_holdings_count is None:
        logger.warning("[ACCOUNT_RECONCILE][SKIP] reason=balance_snapshot_invalid env=%s", env_name)
        return None

    db_positions: list[dict] = []
    try:
        logger.info("[ACCOUNT_RECONCILE][POSITIONS_LOOKUP][START] env=%s market=KRX", env_name)
        os.environ["PB1_LAST_STAGE"] = "account_reconcile.positions_lookup.start"
        db_positions = positions_repo.list_positions(env_name, "pb1_pullback_close")
        os.environ["PB1_LAST_STAGE"] = "account_reconcile.positions_lookup.done"
        logger.info(
            "[ACCOUNT_RECONCILE][POSITIONS_LOOKUP][END] env=%s rows=%s",
            env_name,
            len(db_positions or []),
        )
    except Exception as exc:
        os.environ["PB1_LAST_STAGE"] = "account_reconcile.positions_lookup.skip"
        # traceback_seen 방지: logger.exception() 대신 logger.error() 사용
        logger.error(
            "[ACCOUNT_RECONCILE][POSITIONS_LOOKUP][SKIP] env=%s market=KRX err_type=%s err=%s",
            env_name,
            type(exc).__name__,
            exc,
        )
        return {
            "env": env_name,
            "kis_holdings_count": kis_holdings_count,
            "db_positions_count": None,
            "reset_performed": False,
            "skipped": True,
            "reason": "positions_lookup_failed",
        }
    db_positions_count = sum(1 for row in db_positions if int((row or {}).get("qty") or 0) > 0)
    logger.info(
        "[ACCOUNT_RECONCILE][COUNTS] kis_holdings=%s db_positions=%s env=%s account=%s",
        kis_holdings_count,
        db_positions_count,
        env_name,
        get_masked_account_key(env=env_name, kis=kis),
    )
    if kis_holdings_count != 0 or db_positions_count <= 0:
        return {
            "env": env_name,
            "kis_holdings_count": kis_holdings_count,
            "db_positions_count": db_positions_count,
            "reset_performed": False,
        }

    logger.warning(
        "[ACCOUNT_RECONCILE][MISMATCH] kis_holdings=%s db_positions=%s env=%s",
        kis_holdings_count,
        db_positions_count,
        env_name,
    )
    if os.getenv("AUTO_RECONCILE_PRACTICE_ACCOUNT") != "1":
        logger.warning("[ACCOUNT_RECONCILE][ACTION_REQUIRED] set RESET_PRACTICE_ACCOUNT=1 and run reset script")
        return {
            "env": env_name,
            "kis_holdings_count": kis_holdings_count,
            "db_positions_count": db_positions_count,
            "reset_performed": False,
        }

    account_key = _resolve_practice_account_key(kis, env_name)
    try:
        reset_repo = PracticeAccountResetRepo(engine)
        cleared_counts = reset_repo.clear_account_state(env=env_name, account_key=account_key)
        reset_repo.insert_reset_ledger_event(
            env=env_name,
            account_key=account_key,
            capital_krw=PAPER_MAX_CAPITAL_KRW,
            cleared_counts=cleared_counts,
        )
        logger.warning("[ACCOUNT_RECONCILE][RESET_DB_POSITIONS] reason=kis_empty_db_nonempty")
        return {
            "env": env_name,
            "kis_holdings_count": kis_holdings_count,
            "db_positions_count": db_positions_count,
            "reset_performed": True,
            "cleared_counts": cleared_counts,
        }
    except Exception as exc:
        logger.exception("[ACCOUNT_RECONCILE][RESET_FAIL] env=%s err=%s", env_name, exc)
        return {
            "env": env_name,
            "kis_holdings_count": kis_holdings_count,
            "db_positions_count": db_positions_count,
            "reset_performed": False,
            "error": str(exc),
        }


def _is_balance_empty(balance_snapshot: dict | None) -> bool:
    if not balance_snapshot:
        return False
    rows = balance_snapshot.get("output1") or []
    if not rows:
        return True
    for row in rows:
        try:
            qty = int(float(str(row.get("hldg_qty") or row.get("ord_psbl_qty") or "0").replace(",", "")))
        except Exception:
            qty = 0
        if qty > 0:
            return False
    return True


def should_degrade(remaining_s: float) -> bool:
    return remaining_s < 60



def _handle_balance_unknown_precheck(
    *,
    balance_state: str,
    require_balance_for_entry: bool,
    allow_compute_without_kis: bool,
    order_allowed: bool,
    entry_block_reason: str | None,
) -> tuple[bool, bool, str | None, str | None]:
    """Resolve balance-unknown precheck without blocking fail-soft exits.

    Returns (continue_to_engine, order_allowed, entry_block_reason, return_reason).
    """
    if not (balance_state == BALANCE_STATE_UNKNOWN and require_balance_for_entry and not allow_compute_without_kis):
        return True, order_allowed, entry_block_reason, None
    fail_soft_active = env_bool("KR_BALANCE_FAIL_SOFT_ACTIVE", False)
    fail_soft_exit_allowed = env_bool("EXIT_ALLOWED", False)
    fail_soft_entry_allowed = env_bool("ENTRY_ALLOWED", False)
    if fail_soft_active and fail_soft_exit_allowed:
        logger.warning(
            "[PB1][BALANCE_FAIL_SOFT][CONTINUE] reason=balance_unknown entry_allowed=%d exit_allowed=%d",
            int(bool(fail_soft_entry_allowed)),
            int(bool(fail_soft_exit_allowed)),
        )
        return True, False, entry_block_reason or "BALANCE_FAIL_SOFT_ENTRY_DISABLED", None
    return False, order_allowed, entry_block_reason, "DEGRADED_BALANCE_UNKNOWN"

def run_once(
    *,
    args: argparse.Namespace,
    engine,
    ctx: RunContext,
    loop_mode: bool = False,
    window: WindowDecision | None = None,
    runtime_dir: Path | None = None,
    max_seconds: int = 0,
    runs_ledger_fail_open: bool = False,
) -> tuple[list[Path], bool, dict[str, int], str, str]:
    if (os.getenv("MODE") or "").strip().lower() == "trade" and not (os.getenv("PB1_UNIVERSE_STRATEGY") or "").strip():
        os.environ["PB1_UNIVERSE_STRATEGY"] = "pb1_watchlist_final_scored"

    # ✅ Initialize universe_strategy with default value
    universe_strategy = os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
    env_strategy = (str(os.getenv("STRATEGY_ENV") or ctx.env or "practice").strip().lower() or "practice")
    env_kis = (str(os.getenv("KIS_ENV") or "").strip().lower() or env_strategy or "practice")
    env_derived = env_kis or env_strategy or "practice"
    env_effective = env_derived

    logger.info(
        "[RUN_ONCE][CTX_PREFLIGHT] strategy_env=%s kis_env=%s derived_env=%s effective_env=%s",
        env_strategy,
        env_kis,
        env_derived,
        env_effective,
    )
    logger.info(
        "[PB1][ENV_DERIVE] strategy_env=%s kis_env=%s derived_env=%s effective_env=%s",
        env_strategy,
        env_kis,
        env_derived,
        env_effective,
    )
    
    workflow_run_id = None
    for env_var in ["GITHUB_RUN_ID", "GITHUB_RUN_NUMBER", "WORKFLOW_RUN_ID"]:
        value = os.getenv(env_var)
        if value:
            workflow_run_id = str(value)
            break
    
    # [NEW] FORCE_RUN, WATCHLIST_MODE 로깅
    force_run = env_bool("FORCE_RUN", default=False)
    watchlist_mode = env_bool("WATCHLIST_MODE", default=False)
    # ✅ dry_run은 intended_live 결정 후 LIVE_ENV_LOCK에서 단 한 번만 파싱
    # dry_run = env_bool("DRY_RUN", default=True)  # ← 삭제 (중복 파싱 금지)
    live_trading = env_bool("LIVE_TRADING_ENABLED", default=False)
    
    if force_run or watchlist_mode:
        logger.info(
            "[PB1][FORCE_RUN] FORCE_RUN=%s WATCHLIST_MODE=%s WATCHLIST=%s LIVE_TRADING=%s",
            force_run,
            watchlist_mode,
            os.getenv("WATCHLIST", "")[:100],
            live_trading,
        )
    
    # ✅ [1] intended_live 결정 (STRATEGY_MODE=LIVE 여부)
    intended_live = (os.getenv("STRATEGY_MODE") == "LIVE") or _forced_close_live_execution_enabled()
    
    # ✅ [2] LIVE_ENV_LOCK 호출 → dry_run 파싱 (단 한 번만)
    dry_run = _force_live_env_lock_if_needed(intended_live=intended_live)
    dry_run_from_alias = _env_bool_any(("DRY_RUN", "DRYRUN"), default=True)
    disable_live_trading = _env_bool_any(("DISABLE_LIVE_TRADING",), default=False)
    live_trading_enabled = _env_bool_any(("LIVE_TRADING_ENABLED",), default=False)
    simulation_mode = _env_bool_any(("SIMULATION_MODE",), default=False)
    logger.info(
        "[LIVE_ENV_LOCK] dry_run=%s disable_live_trading=%s live_trading_enabled=%s simulation_mode=%s intended_live=%s raw_dry_run=%s raw_dryrun=%s raw_disable_live=%s",
        dry_run_from_alias,
        disable_live_trading,
        live_trading_enabled,
        simulation_mode,
        intended_live,
        os.getenv("DRY_RUN"),
        os.getenv("DRYRUN"),
        os.getenv("DISABLE_LIVE_TRADING"),
    )

    compute_only_flags = _compute_only_full_run_flags(dry_run=dry_run_from_alias)
    compute_only_full_run = compute_only_flags["enabled"]
    logger.info(
        "[COMPUTE_ONLY][FULL_RUN] enabled=%s dry_run=%s force_block_live=%s disable_live=%s force_compute=%s bypass_cutoff=%s",
        int(compute_only_full_run),
        int(compute_only_flags["dry_run"]),
        int(compute_only_flags["force_block_live"]),
        int(compute_only_flags["disable_live_trading"]),
        int(compute_only_flags["force_compute"]),
        int(compute_only_flags["bypass_cutoff"]),
    )
    
    # ✅ [3] LIVE mode 검증 (dry_run은 이미 파싱 완료)
    if intended_live:
        violations = []
        if os.getenv("LIVE_TRADING_ENABLED") != "1":
            violations.append("LIVE_TRADING_ENABLED != '1'")
        if os.getenv("DISABLE_LIVE_TRADING") == "1":
            violations.append("DISABLE_LIVE_TRADING == '1'")
        if dry_run:  # ✅ 이미 파싱된 값 사용 (env 재파싱 금지)
            violations.append(f"DRY_RUN=True (env={os.getenv('DRY_RUN')})")
        if os.getenv("DB_ONLY") == "1":
            violations.append("DB_ONLY == '1'")
        if os.getenv("NONTRADING_SMOKE") == "1":
            violations.append("NONTRADING_SMOKE == '1'")
        
        # ✅ FIX C: LIVE 플래그 충돌 시 DIAG로 강등 (abort 대신)
        if violations and not _forced_close_live_execution_enabled():
            logger.error(
                "="*80
            )
            logger.error("[PB1][LIVE][CONFLICT] CRITICAL: intended_live=True but conflicts detected:")
            for v in violations:
                logger.error(f"  - {v}")
            logger.error("  ACTION: Downgrading mode to DIAG, blocking all orders, calculation mode only")
            logger.error("="*80
            )
            # 충돌이면 intended_live를 false로 내리기 (다음 로직에서 mode를 DIAG로 강등)
            intended_live = False
    
    now = _get_now_kst()
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    tick_start_ts = time_mod.monotonic()
    deadline_ts = tick_start_ts + max_seconds if max_seconds > 0 else None
    persist_budget_sec = _parse_int_env("PB1_PERSIST_BUDGET_SEC", 30)
    if max_seconds > 0:
        persist_budget_sec = max(5, min(persist_budget_sec, max_seconds))
    else:
        persist_budget_sec = max(5, persist_budget_sec)
    trade_budget_sec = max(0, max_seconds - persist_budget_sec) if max_seconds > 0 else 0
    run_start_ts = time_mod.time()
    
    # IMPORTANT:
    # Do NOT create a separate after-hours / weekend / smoke trading engine.
    # The same intraday trade path must be reused for market hours, after-hours compute-only,
    # and non-trading-day validation. Only order submission gates may differ.
    # 중요:
    # 장중 공통 매매 로직을 훼손하지 않는다.
    # 장마감 후/비거래일 검증도 동일한 trade 경로를 사용하며,
    # 달라질 수 있는 것은 주문 제출 허용 여부뿐이다.
    # ✅ CRITICAL: Trade는 장중에 "전일 영업일 derived"를 사용
    if not env_effective:
        raise RuntimeError("[RUN_ONCE][ENV] env_effective is empty before resolve_trade_context")
    if is_kr_market():
        trade_date = resolve_kr_trade_date(now)
        as_of_date = resolve_kr_expected_as_of(trade_date)
        trade_ctx = {"trade_date": trade_date.isoformat(), "as_of": as_of_date.isoformat(), "reason": "KR_CALENDAR"}
    else:
        trade_ctx = resolve_trade_context(now=now, env=env_effective)
        trade_date = date.fromisoformat(str(trade_ctx["trade_date"]))
    as_of = str(trade_ctx["as_of"])
    asof_reason = str(trade_ctx["reason"])
    run_ctx: dict[str, Any] = {
        "trade_date": trade_date.isoformat(),
        "derived_as_of": as_of,
        "trade_context": dict(trade_ctx),
        "window_name": "day",
        "phase_name": "entry",
        "final30_source": None,
        "final30_locked": False,
        "final30_rows": 0,
        "final30_as_of": None,
        "final30_codes": [],
    }
    os.environ["AS_OF_OVERRIDE"] = as_of
    
    logger.info(
        "[ASOF][RUN_ONCE] trade_date=%s derived_as_of=%s reason=%s",
        trade_date.isoformat(),
        as_of,
        asof_reason,
    )
    logger.info("[ASOF][LOCK] trade_date=%s derived_as_of=%s immutable=1 source=db_contract", trade_date.isoformat(), as_of)
    
    runtime_root_dir = runtime_dir or runtime_root()
    smoke_enabled = os.getenv("PB1_SMOKE_RUN") == "1"
    close_cancel_only = env_bool("PB1_CLOSE_CANCEL_ONLY", False)
    if close_cancel_only:
        logger.info("[PB1][MODE] close_cancel_only=Y")
    event_name = os.getenv("GITHUB_EVENT_NAME", "") or ""
    event_name_lower = event_name.lower()
    mode, trading_day, market_window, mode_source = resolve_strategy_mode(
        now_kst=now,
        force_mode_env=os.getenv("FORCE_STRATEGY_MODE"),
    )
    if _forced_close_live_execution_enabled():
        mode = "LIVE"
        mode_source = "forced_close_live"
        market_window = "close"
        logger.info("[PB1][FORCED_CLOSE_LIVE] mode=LIVE market_window=close phase=exit")
    trading_day_detected = trading_day
    if os.getenv("TRADING_DAY") is not None:
        trading_day = _env_bool("TRADING_DAY", default=trading_day)
    auto_window = decide_market_window(now)
    if auto_window != market_window:
        logger.info(
            "[PB1][WINDOW][AUTO] now_kst=%s env_window=%s computed_window=%s -> using computed",
            now.isoformat(),
            market_window,
            auto_window,
        )
        market_window = auto_window
    
    # ✅ FIX C: LIVE 플래그 충돌 감지 후 mode 강등
    if mode == "LIVE" and not intended_live and not _forced_close_live_execution_enabled():
        logger.error("="*80)
        logger.error("[PB1][LIVE][DOWNGRADE] Downgrading mode from LIVE to DIAG due to LIVE flag conflicts")
        logger.error("="*80)
        mode = "DIAG"
    
    effective_mode = mode
    if smoke_enabled:
        trading_day = True
    diag_rehearsal_active = _env_bool("PB1_DIAG_REHEARSAL", False) and _env_bool("DIAG_ALLOW_NONTRADING_RUN", False)
    if diag_rehearsal_active and not trading_day:
        logger.warning(
            "[PB1][DIAG] non-trading-day override enabled (trading_day=%s -> forced True)",
            trading_day_detected,
        )
        trading_day = True
    logger.info(
        "[MODE_DECISION] source=%s now_kst=%s trading_day=%s window=%s mode=%s",
        mode_source,
        now.isoformat(),
        trading_day,
        market_window,
        mode,
    )
    os.environ["STRATEGY_MODE"] = mode
    diag_env_flag = (
        env_bool("DIAGNOSTIC_FORCE_RUN", False)
        or env_bool("DIAGNOSTIC_ONLY", DIAGNOSTIC_ONLY)
        or env_bool("DIAGNOSTIC_MODE", DIAGNOSTIC_MODE)
    )
    open_dt, close_dt = _market_session(now)
    allow_wait = env_bool("PB1_ALLOW_WAIT", env_bool("PB1_WAIT_FOR_WINDOW", PB1_WAIT_FOR_WINDOW))
    max_wait_s = int(PB1_MAX_WAIT_FOR_WINDOW_MIN) * 60
    entry_flag = parse_env_flag("PB1_ENTRY_ENABLED", default=PB1_ENTRY_ENABLED)
    force_phase_env = os.getenv("FORCE_PB1_PHASE") or ""
    if _resolve_session_kind() == "close" or force_phase_env.strip().lower() == "close":
        os.environ["FORCE_MARKET_WINDOW"] = "close"
        force_phase_env = "close"
        market_window = "close"
        entry_flag = parse_env_flag("PB1_ENTRY_ENABLED", default=False)
        logger.info("[PB1][PHASE] phase=close reason=forced_close_session entry_enabled=0 exit_enabled=1 close_enabled=1")
    phase_seed = force_phase_env if force_phase_env else (None if args.phase == "auto" else args.phase)
    resolved_window, window_label, resolved_phase, phase_reason, phase_window, context_reasons = _resolve_market_context(
        now=now,
        trading_day=trading_day,
        market_window=market_window,
        window_override=args.window,
        phase_seed=phase_seed,
    )
    force_entry_window_override = _env_flag("PB1_FORCE_ENTRY_WINDOW_OVERRIDE", default=False)
    session_recovery_continue = _env_flag("PB1_SESSION_RECOVERY_CONTINUE", default=False)
    forced_trade_session = str(os.getenv("PB1_FORCED_TRADE_SESSION") or "").strip().lower()
    phase_guard_classification = str(os.getenv("PB1_PHASE_GUARD_CLASSIFICATION") or "").strip()
    if force_entry_window_override and forced_trade_session in {"am", "pm"}:
        market_window = "day"
        window_label = "day"
        resolved_phase = "entry"
        phase_reason = "force_entry_window_override"
        context_reasons.append("trade_session_entry_override")
    normalized_window_name, normalized_phase_name = _normalize_window_phase(
        raw_window=resolved_window,
        market_window=market_window,
        phase=resolved_phase,
    )
    run_ctx["window_name"] = normalized_window_name
    run_ctx["phase_name"] = normalized_phase_name
    run_ctx["force_entry_window_override"] = force_entry_window_override
    run_ctx["session_recovery_continue"] = session_recovery_continue
    run_ctx["forced_trade_session"] = forced_trade_session
    run_ctx["phase_guard_classification"] = phase_guard_classification
    logger.info("[ASOF][USE] component=runner value=%s source=run_ctx", run_ctx["derived_as_of"])
    
    manual_test_reasons = _manual_test_route_reasons(mode=mode)
    manual_test_route = bool(manual_test_reasons)

    # ✅ DIAG_FULL_EXEC: DIAG 모드에서 window/phase 강제 우회
    diag_full_exec = env_bool("PB1_DIAG_FULL_EXEC", False)
    if diag_full_exec and mode == "DIAG":
        diag_phase = force_phase_env if force_phase_env else "prep"
        diag_window = market_window if market_window in {"preopen", "morning", "day", "close", "after"} else "day"
        logger.info("[PB1][DIAG_FULL_EXEC] force window=%s, phase=%s (bypass window/phase gates)", diag_window, diag_phase)
        market_window = diag_window
        window_label = diag_window
        resolved_phase = diag_phase
        phase_reason = "diag_full_exec_override"
        context_reasons.append("diag_full_exec:forced_" + diag_window + "_" + diag_phase)

    after_close_entry_dryrun = _after_close_entry_dryrun_enabled(now)

    if after_close_entry_dryrun:
        # live gate 이중 잠금
        os.environ["LIVE_TRADING_ENABLED"] = "0"
        os.environ["FORCE_BLOCK_LIVE"] = "1"
        logger.warning(
            "[PB1][DIAG_FULL_EXEC] enabled=1 after_close_entry_dryrun=1 now_kst=%s strategy_mode=%s dry_run=%s force_block_live=%s live_trading_enabled=0",
            now.strftime("%H%M"),
            os.getenv("STRATEGY_MODE"),
            os.getenv("DRY_RUN"),
            os.getenv("FORCE_BLOCK_LIVE"),
        )
        logger.warning(
            "[PB1][AFTER_CLOSE_ENTRY_DRYRUN][ENABLED] now_kst=%s mode=%s force_window=%s force_phase=%s dry_run=%s force_block_live=%s",
            now.isoformat(),
            os.getenv("STRATEGY_MODE"),
            os.getenv("FORCE_MARKET_WINDOW"),
            os.getenv("FORCE_PB1_PHASE"),
            os.getenv("DRY_RUN"),
            os.getenv("FORCE_BLOCK_LIVE"),
        )
    
    if window is not None:
        resolved_window = window
        window_label = _resolve_window_label(market_window, window)
        context_reasons.append("window:locked")
    window = resolved_window
    phase_for_log = resolved_phase
    normalized_window_name, normalized_phase_name = _normalize_window_phase(
        raw_window=window,
        market_window=market_window,
        phase=phase_for_log,
    )
    run_ctx["window_name"] = normalized_window_name
    run_ctx["phase_name"] = normalized_phase_name

    os.environ.setdefault("MORNING_WINDOW_START", MORNING_WINDOW_START)
    os.environ.setdefault("MORNING_WINDOW_END", MORNING_WINDOW_END)
    os.environ.setdefault("MORNING_EXIT_START", MORNING_EXIT_START)
    os.environ.setdefault("MORNING_EXIT_END", MORNING_EXIT_END)
    os.environ.setdefault("AFTERNOON_WINDOW_START", AFTERNOON_WINDOW_START)
    os.environ.setdefault("AFTERNOON_WINDOW_END", AFTERNOON_WINDOW_END)
    os.environ.setdefault("CLOSE_AUCTION_START", CLOSE_AUCTION_START)
    os.environ.setdefault("CLOSE_AUCTION_END", CLOSE_AUCTION_END)

    action = "run"
    target_start = None
    if not loop_mode:
        action, target_start = _decide_action(now, trading_day, open_dt, close_dt, allow_wait, max_wait_s, smoke_enabled)
        if manual_test_route and action != "run":
            for reason in manual_test_reasons:
                logger.info("[PB1][MANUAL_TEST_ROUTE] reason=%s -> force action=run", reason)
            action = "run"
            target_start = None
        if market_window == "after" and compute_only_full_run:
            action = "run"
            target_start = None
            resolved_phase = "entry"
            phase_for_log = "entry"
            phase_reason = "after_compute_only_full_run"
            context_reasons.append("after_compute_only:existing_live_pipeline")
        trade_run_minervini = env_bool("TRADE_RUN_MINERVINI", default=False)
        if trade_run_minervini and mode == "DIAG" and action == "wait":
            logger.info(
                "[PB1][TRADE_MINERVINI] mode=DIAG TRADE_RUN_MINERVINI=1 -> bypass wait and run now",
            )
            action = "run"
            target_start = None
        logger.info(
            "[PB1][RUN-PLAN] event=%s now_kst=%s trading_day=%s action=%s target_start=%s max_wait_s=%s window=%s phase=%s allow_wait=%s",
            event_name_lower or "unknown",
            now.isoformat(),
            trading_day,
            action,
            target_start.isoformat() if target_start else "none",
            max_wait_s,
            window_label,
            phase_for_log,
            allow_wait,
        )

        if action == "wait" and target_start:
            while True:
                now = _get_now_kst()
                remaining = (target_start - now).total_seconds()
                if remaining <= 0:
                    break
                if remaining > max_wait_s:
                    logger.info(
                        "[PB1][WAIT] now_kst=%s next_open_kst=%s sleeping_s=0 reason=exceeds_max",
                        now.isoformat(),
                        target_start.isoformat(),
                    )
                    action = "smoke"
                    break
                sleep_for = remaining if remaining < 30 else min(60, remaining)
                logger.info("[PB1][WAIT] now_kst=%s next_open_kst=%s sleeping_s=%.0f", now.isoformat(), target_start.isoformat(), sleep_for)
                time_mod.sleep(sleep_for)
            if action == "wait":
                now = _get_now_kst()
                mode, trading_day, market_window, mode_source = resolve_strategy_mode(
                    now_kst=now,
                    force_mode_env=os.getenv("FORCE_STRATEGY_MODE"),
                )
                effective_mode = mode
                os.environ["STRATEGY_MODE"] = mode
                resolved_window, window_label, resolved_phase, phase_reason, phase_window, context_reasons = _resolve_market_context(
                    now=now,
                    trading_day=trading_day,
                    market_window=market_window,
                    window_override=args.window,
                    phase_seed=phase_seed,
                )
                force_entry_window_override = _env_flag("PB1_FORCE_ENTRY_WINDOW_OVERRIDE", default=False)
                session_recovery_continue = _env_flag("PB1_SESSION_RECOVERY_CONTINUE", default=False)
                forced_trade_session = str(os.getenv("PB1_FORCED_TRADE_SESSION") or "").strip().lower()
                phase_guard_classification = str(os.getenv("PB1_PHASE_GUARD_CLASSIFICATION") or "").strip()
                if force_entry_window_override and forced_trade_session in {"am", "pm"}:
                    market_window = "day"
                    window_label = "day"
                    resolved_phase = "entry"
                    phase_reason = "force_entry_window_override"
                    context_reasons.append("trade_session_entry_override")
                window = resolved_window
                phase_for_log = resolved_phase
                logger.info(
                    "[MODE_DECISION] source=%s now_kst=%s trading_day=%s window=%s mode=%s",
                    mode_source,
                    now.isoformat(),
                    trading_day,
                    market_window,
                    mode,
                )
                logger.info(
                    "[PB1][PHASE] now=%s window=%s phase=%s reason=%s entry_enabled=%s phase_window=%s",
                    now.isoformat(),
                    window_label,
                    resolved_phase,
                    phase_reason,
                    entry_flag.value,
                    phase_window,
                )
                action = "run" if trading_day else "smoke"
                logger.info("[PB1][WAIT][DONE] now_kst=%s window=%s phase=%s", now.isoformat(), window_label, phase_for_log)
    
    # ✅ 장외 스킵은 LIVE 모드에서만 적용 (DIAG는 계속 실행)
    if not trading_day:
        # DIAG 모드: 장외라도 엔진 실행 (단 KIS API는 차단됨)
        if mode == "DIAG":
            logger.info(
                "[PB1][DIAG][NONTRADING_DAY] mode=DIAG continue execution (KIS blocked)",
            )
            # nontrading_smoke는 실행하되, 엔진도 계속 진행
            exit_status = "DIAG_NONTRADING_CONTINUE"
            
            # ✅ CRITICAL: DIAG 모드에서도 run 시작 시 lock된 derived_as_of를 그대로 사용
            as_of = str(run_ctx.get("derived_as_of") or as_of)
            
            logger.info(
                "[ASOF][DIAG][NONTRADING] trade_date=%s derived_as_of=%s",
                now.date().isoformat(),
                as_of,
            )
            
            smoke_flag = nontrading_smoke_flag_path(runtime_root_dir, as_of=as_of)
            if not smoke_flag.exists() or NONTRADING_SMOKE_FORCE:
                try:
                    run_nontrading_smoke_once(
                        runtime_root_dir=runtime_root_dir,
                        engine=engine,
                        now=now,
                        env=(os.getenv("KIS_ENV") or "practice").lower(),
                        strategy=os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY,
                        as_of=as_of,
                        timeout_sec=NONTRADING_SMOKE_TIMEOUT_SEC,
                        db_store=NONTRADING_SMOKE_DB_STORE,
                        force_rebuild=NONTRADING_SMOKE_FORCE_REBUILD,
                    )
                    write_nontrading_smoke_flag(
                        runtime_root_dir,
                        now=now,
                        run_id=os.getenv("TRADER_RUN_ID", "local"),
                        sha=os.getenv("GITHUB_SHA", "unknown"),
                        as_of=as_of,
                    )
                except Exception as exc:
                    logger.warning("[NONTRADING_SMOKE][FAIL] err=%s", exc)
            # ✅ DIAG는 계속 실행 (return 하지 않음)
        elif compute_only_full_run or bool(compute_only_flags.get("force_block_live")) or dry_run:
            logger.info(
                "[PB1][NONTRADING][COMPUTE] continue same trade path order_blocked=1 derived_as_of=%s",
                str(run_ctx.get("derived_as_of") or as_of),
            )
            market_window = "after"
            window_label = "after"
            run_ctx["window_name"] = "after"
            run_ctx["phase_name"] = "entry"
        else:
            # LIVE 모드: 장외면 스킵
            exit_status = "NONTRADING_DAY_EXIT"
            if loop_mode:
                logger.info("[PB1][LOOP] non-trading-day -> skip (LIVE mode)")
            else:
                logger.info("[PB1][SKIP] non-trading-day -> skip (LIVE mode)")
            logger.info(
                "[PB1][EXIT] reason=nontrading_day_exit mode=LIVE",
            )
            return [], False, {}, phase_for_log, exit_status

    # ✅ DIAG + PB1_DIAG_FULL_EXEC=1이면 smoke 건너뛰고 엔진 실행
    diag_full = os.getenv("PB1_DIAG_FULL_EXEC", "0") in ("1", "true", "TRUE", "yes", "YES")
    if action == "smoke" and mode == "DIAG" and (diag_full or manual_test_route):
        logger.info("[PB1][DIAG_FULL_EXEC] bypass smoke -> run engine once (no KIS HTTP)")
        action = "run"  # smoke 건너뛰고 엔진 실행
    elif action == "smoke" and not compute_only_full_run:
        if not trading_day:
            session_kind = str(os.getenv("PB1_SESSION_KIND") or "unknown")
            logger.info(
                "[TRADE][TRADING_DAY][CHECK] date=%s is_trading_day=0 reason=weekend_or_holiday",
                now.date().isoformat(),
            )
            logger.info(
                "[TRADE][TRADING_DAY][SKIP] reason=non_trading_day session=%s",
                session_kind,
            )
            logger.info(
                "[RUN_SUMMARY][RESULT] status=OK_NO_TRADE reason=NON_TRADING_DAY session=%s",
                session_kind,
            )
            logger.info("[PB1][EXIT] reason=non_trading_day")
            return [], False, {}, phase_for_log, "OK_NO_TRADE"
        os.environ["KIS_HTTP_CALLER_ROUTE"] = "smoke"
        _run_smoke(engine, kis_env=(os.getenv("KIS_ENV") or "practice").lower(), now=now)
        return [], False, {}, phase_for_log, "SMOKE"
    elif action == "smoke" and compute_only_full_run and ((run_ctx.get("window_name") == "after") or market_window == "after"):
        logger.info("[AFTER_COMPUTE_ONLY][ROUTE] reuse existing intraday trade pipeline")
        action = "run"

    # ✅ DIAG_FULL이면 윈도우 게이트 무시하고 계속 진행
    if not window and not close_cancel_only:
        if mode == "DIAG" and (diag_full or manual_test_route):
            phase_default = os.getenv("PB1_PHASE_DEFAULT", "entry")
            window = None
            window_label = market_window if market_window in {"preopen", "morning", "day", "close", "after"} else "day"
            phase_for_log = phase_default
            run_ctx["window_name"] = window_label
            run_ctx["phase_name"] = "entry" if phase_default not in {"entry", "exit", "manage"} else phase_default
            logger.info("[PB1][DIAG_FULL_EXEC] override window gate -> proceed (window=%s, phase=%s)", window_label, phase_default)
        elif compute_only_full_run and market_window == "after":
            resolved_phase = "entry"
            phase_for_log = "entry"
            phase_reason = "after_compute_only_full_run"
            logger.info("[AFTER_COMPUTE_ONLY][ROUTE] reuse existing intraday trade pipeline")
        else:
            logger.info("[PB1][WINDOW] outside active windows override=%s now=%s", args.window, now)
            return [], False, {}, phase_for_log, "OUTSIDE_WINDOW"

    non_trading_day = not trading_day
    force_diag = diag_env_flag
    diag_enabled = force_diag  # ✅ 호환성을 위해 추가

    # ✅ CRITICAL: intended_live와 dry_run은 run_once에서 이미 확정됨
    # 여기서는 재계산하지 말고 env에서 그대로 읽기만 (이미 락됨)
    
    dry_run = _env_bool_any(("DRY_RUN", "DRYRUN"), default=True)
    intended_live = (os.getenv("STRATEGY_MODE") == "LIVE")
    
    logger.info(
        "[TICK][INIT] intended_live=%s dry_run=%s (from locked env)",
        intended_live,
        dry_run,
    )
    
    # ✅ intended_live를 환경변수로 저장 (주문 함수에서 접근 가능하도록)
    os.environ["INTENDED_LIVE"] = "1" if intended_live else "0"
    
    
    expect_live_flag = env_bool("EXPECT_LIVE_TRADING", False)
    mode_resolved = resolve_mode(os.getenv("STRATEGY_MODE", ""))
    disable_live_flag_value = _env_bool_any(("DISABLE_LIVE_TRADING",), default=False)
    live_trading_flag_value = _env_bool_any(("LIVE_TRADING_ENABLED",), default=False)
    
    expect_kis_env = (os.getenv("EXPECT_KIS_ENV") or "").strip().lower() or None
    env_kis_raw = (os.getenv("KIS_ENV") or "").strip()
    api_base_url = (os.getenv("API_BASE_URL") or "").lower()
    guard_live = expect_live_flag and not diag_enabled and trading_day and not dry_run
    if guard_live:
        guard_failures: list[str] = []
        if dry_run:
            guard_failures.append("dry_run")
        if not live_trading_flag_value:
            guard_failures.append("LIVE_TRADING_ENABLED!=1")
        if disable_live_flag_value:
            guard_failures.append("DISABLE_LIVE_TRADING!=0")
        if mode_resolved != "LIVE":
            guard_failures.append("STRATEGY_MODE!=LIVE")
        if env_effective != "practice":
            guard_failures.append("KIS_ENV!=practice")
        if "openapivts" not in api_base_url:
            guard_failures.append("API_BASE_URL missing openapivts")
        if expect_kis_env and env_kis != expect_kis_env:
            guard_failures.append("EXPECT_KIS_ENV mismatch")
        if guard_failures:
            raise SystemExit(f"EXPECT_LIVE_TRADING=1 guards failed: {guard_failures}")

    # ✅ _apply_env_flags는 intended_live=True일 때 스킵 (env 이미 락됨)
    # DIAG 모드나 기타 경우에만 env 업데이트 허용
    def _apply_env_flags_if_needed(dry: bool) -> None:
        """
        Apply environment flags ONLY if not already locked by intended_live.
        If intended_live=True, the env was locked by _force_live_env_lock_if_needed.
        DO NOT overwrite the lock.
        """
        if intended_live:
            # ✅ Already locked - do not touch
            logger.info("[ENV_FLAGS] Skip _apply_env_flags (intended_live=True, env locked)")
            return
        
        # ✅ Safe to apply for non-live scenarios
        os.environ["DRY_RUN"] = "1" if dry else "0"
        os.environ["DISABLE_LIVE_TRADING"] = "1" if disable_live_flag_value else "0"
        os.environ["LIVE_TRADING_ENABLED"] = "1" if live_trading_flag_value else "0"
        os.environ["STRATEGY_MODE"] = effective_mode

    force_phase_env = os.getenv("FORCE_PB1_PHASE") or ""
    phase_override_arg = resolved_phase
    
    if (
        window_label
        and event_name_lower == "push"
        and args.phase == "auto"
        and not force_phase_env
        and window_label == "day"
        and env_bool("PB1_FORCE_ENTRY_ON_PUSH", PB1_FORCE_ENTRY_ON_PUSH)
    ):
        try:
            start = datetime.fromisoformat(f"{now.date()}T{PB1_MORNING_WINDOW_END}")
            end = datetime.fromisoformat(f"{now.date()}T{PB1_ENTRY_WINDOW_END}")
            in_afternoon = start.time() <= now.time() < end.time()
        except Exception:
            in_afternoon = False
        if trading_day and in_afternoon:
            logger.info("[PB1][PHASE_OVERRIDE] event=push from=%s to=entry reason=PB1_FORCE_ENTRY_ON_PUSH", phase_override_arg)
            phase_override_arg = "entry"
            phase_reason = "force_push"

    if diag_enabled:
        dry_run = True
        if args.phase == "auto" and not force_phase_env and not force_entry_window_override:
            phase_override_arg = "verify"
            phase_reason = "diagnostic"
        window = None
        window_label = "day"
        _apply_env_flags_if_needed(dry_run)

    if force_entry_window_override and forced_trade_session in {"am", "pm"}:
        market_window = "day"
        window_label = "day"
        phase_override_arg = "entry"
        phase_for_log = "entry"
        phase_reason = "force_entry_window_override"
        if "trade_session_entry_override" not in context_reasons:
            context_reasons.append("trade_session_entry_override")

    if compute_only_full_run and market_window == "after":
        phase_override_arg = "entry"
        phase_for_log = "entry"
        phase_reason = "after_compute_only_full_run"
        logger.info("[AFTER_COMPUTE_ONLY][ROUTE] reuse existing intraday trade pipeline")

    window_label = window_label or _resolve_window_label(market_window, window)
    phase_for_log = phase_override_arg or "none"

    normalized_window_name, normalized_phase_name = _normalize_window_phase(
        raw_window=window_label,
        market_window=market_window,
        phase=phase_for_log,
    )
    run_ctx["window_name"] = normalized_window_name
    run_ctx["phase_name"] = normalized_phase_name

    context_reasons = [r for r in context_reasons if not r.startswith("window:") and not r.startswith("phase:")]
    context_reasons.append(f"window:{window_label}")
    context_reasons.append(f"phase:{phase_for_log}")
    logger.info(
        "[PB1][PHASE] now=%s window=%s phase=%s reason=%s entry_enabled=%s phase_window=%s",
        now.isoformat(),
        window_label,
        phase_for_log,
        phase_reason,
        entry_flag.value,
        phase_window,
    )

    logger.info(
        "[PB1][TICK][SESSION_CTX] force_market_window=%s phase=%s derived_as_of=%s",
        os.getenv("FORCE_MARKET_WINDOW"),
        phase_for_log,
        str(run_ctx.get("derived_as_of") or as_of),
    )

    logger.info(
        "[PB1][TICK] now_kst=%s market_window=%s window=%s phase=%s reasons=%s",
        now.isoformat(),
        market_window,
        window_label,
        phase_for_log,
        context_reasons or ["none"],
    )

    pm_should_handoff = _pm_should_handoff_to_close(now)

    if pm_should_handoff and after_close_entry_dryrun:
        logger.warning(
            "[PB1][PM][HANDOFF_TO_CLOSE][BYPASS] now_kst=%s close_start=%s reason=after_close_entry_dryrun",
            now.isoformat(),
            CLOSE_AUCTION_START,
        )
    elif pm_should_handoff:
        logger.info("[PB1][PM][HANDOFF_TO_CLOSE] now_kst=%s close_start=%s", now.isoformat(), CLOSE_AUCTION_START)
        return [], False, {}, phase_for_log, "HANDOFF_TO_CLOSE"

    def _remaining_seconds() -> float:
        if deadline_ts is None:
            return float("inf")
        return max(0.0, deadline_ts - time_mod.monotonic())

    remaining_s = _remaining_seconds()
    close_liquidation_enabled = str(os.getenv("PB1_CLOSE_LIQUIDATION_ENABLED", os.getenv("KR_CLOSE_LIQUIDATION_ENABLED", "0"))).strip().lower() not in {"0", "false", "no"}
    exit_short_circuit = (phase_for_log == "exit" or window_label == "close") and not close_liquidation_enabled
    if (phase_for_log == "exit" or window_label == "close") and close_liquidation_enabled:
        logger.info("[KR_CLOSE][LIQUIDATION][ENGINE_PATH] reason=close_liquidation_enabled skip_exit_shortcircuit=1")

    def _run_close_reconcile_once(*, reason_label: str, kis_obj: KisAPI | None) -> tuple[bool, bool]:
        checkpoint_key = _close_reconcile_checkpoint_key(
            env=(os.getenv("KIS_ENV") or "practice").lower(),
            trade_date=now.date(),
        )
        try:
            checkpoint = load_job_checkpoint(engine, checkpoint_key) or {}
        except Exception as exc:
            logger.warning("[PB1][CLOSE][RECONCILE][CHECKPOINT_LOAD_FAIL] err=%s", exc)
            checkpoint = {}
        if checkpoint.get("done"):
            logger.info("[PB1][CLOSE][RECONCILE][SKIP] reason=already_done")
            return bool(checkpoint.get("reconcile_ok")), bool(checkpoint.get("close_stale_ok"))

        reconcile_ok_local = False
        close_stale_ok_local = False
        reconcile_result: dict[str, Any] = {}
        if kis_obj:
            try:
                ctx = RunContext(
                    run_id=run_id,
                    env=(os.getenv("KIS_ENV") or "practice").lower(),
                    strategy="pb1_pullback_close",
                    started_at=now,
                    dry_run=False,
                )
                reconcile_result = reconcile_today(
                    engine=engine,
                    kis=kis_obj,
                    ctx=ctx,
                ) or {}
                reconcile_ok_local = True
            except Exception:
                logger.exception("[PB1][%s] reconcile_today failed", reason_label.upper())
        try:
            balance_snapshot = kis_obj.get_balance() if kis_obj else {}
        except Exception as exc:
            logger.warning("[PB1][CLOSE][BALANCE][FAIL] reason=%s err=%s", reason_label, exc)
            balance_snapshot = {}
        try:
            sell_fill_codes = {
                str((row or {}).get("code") or "").zfill(6)
                for row in FillsRepo(engine).list_today_sell_fills((os.getenv("KIS_ENV") or "practice").lower())
            }
            close_stale_positions(
                engine=engine,
                env=(os.getenv("KIS_ENV") or "practice").lower(),
                strategy="pb1_pullback_close",
                reason="exit_phase",
                ts=now,
                kis_balance=balance_snapshot,
                sell_fill_codes=sell_fill_codes,
                runtime_dir=runtime_root_dir,
            )
            close_stale_ok_local = True
        except Exception:
            logger.exception("[PB1][%s] close_stale_positions failed", reason_label.upper())
        try:
            save_job_checkpoint(
                engine,
                checkpoint_key,
                {
                    "done": True,
                    "reconcile_ok": bool(reconcile_ok_local),
                    "close_stale_ok": bool(close_stale_ok_local),
                    "reason": reason_label,
                    "guard_reason": reconcile_result.get("guard_reason"),
                    "updated_at": now.isoformat(),
                },
            )
        except Exception as exc:
            logger.warning("[PB1][CLOSE][RECONCILE][CHECKPOINT_SAVE_FAIL] err=%s", exc)
        logger.info("[PB1][CLOSE][RECONCILE][RUN_ONCE] done=1")
        return reconcile_ok_local, close_stale_ok_local

    if exit_short_circuit:
        logger.info("[PB1][EXIT_SHORTCIRCUIT] start remaining_s=%.1f", remaining_s)
        kis = None
        try:
            kis = KisAPI()
        except Exception:
            logger.exception("[PB1][EXIT_SHORTCIRCUIT] KIS init failed")
        run_id = os.getenv("TRADER_RUN_ID", "local")
        reconcile_ok = False
        close_stale_ok = False
        try:
            reconcile_ok, close_stale_ok = _run_close_reconcile_once(reason_label="exit_shortcircuit", kis_obj=kis)
            _write_last_db_write(runtime_root_dir, run_id=run_id, reason="exit_shortcircuit", now=now)
            if not reconcile_ok or not close_stale_ok:
                logger.error(
                    "[PB1][EXIT_SHORTCIRCUIT][PARTIAL_FAIL] reconcile_ok=%s close_stale_ok=%s - continuing with caution",
                    reconcile_ok,
                    close_stale_ok,
                )
            logger.info("[PB1][EXIT_SHORTCIRCUIT] done")
            return [], True, {}, phase_for_log, "EXIT_SHORTCIRCUIT"
        except Exception:
            logger.exception("[PB1][EXIT_SHORTCIRCUIT] failed")

    if should_degrade(remaining_s):
        logger.warning("[PB1][DEGRADED] remaining_s=%.1f -> reconcile+persistent only", remaining_s)
        kis = None
        try:
            kis = KisAPI()
        except Exception:
            logger.exception("[PB1][DEGRADED] KIS init failed")
        run_id = os.getenv("TRADER_RUN_ID", "local")
        reconcile_ok = False
        close_stale_ok = False
        try:
            reconcile_ok, close_stale_ok = _run_close_reconcile_once(reason_label="budget_degraded", kis_obj=kis)
            _write_last_db_write(runtime_root_dir, run_id=run_id, reason="budget_degraded", now=now)
            if not reconcile_ok or not close_stale_ok:
                logger.error(
                    "[PB1][DEGRADED][PARTIAL_FAIL] reconcile_ok=%s close_stale_ok=%s - continuing with caution",
                    reconcile_ok,
                    close_stale_ok,
                )
            return [], True, {}, phase_for_log, "DEGRADED_BUDGET"
        except Exception:
            logger.exception("[PB1][DEGRADED] failed")

    if not loop_mode:
        if is_db_only_mode():
            universe_strategy = os.getenv("PB1_UNIVERSE_STRATEGY_KEY") or os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
            _log_db_only_universe_precheck(
                repo=UniverseRepo(engine),
                env=env_effective,
                strategy=universe_strategy,
            )
        universe_members = ensure_universe_built_once(engine=engine, as_of=as_of)
        if not universe_members:
            logger.warning("[PB1] universe empty -> skip trading cycle")
            return touched_files, False, {}, phase_for_log, "SKIP_EMPTY_UNIVERSE"

    logger.info(
        "[PB1_RUNNER][CFG] phase=%s as_of=%s TRADE_INPUT=%s MIN_BUYABLE=%s RELAX_PASSES=%s RS_MIN=%s VCP_MIN=%s",
        phase_for_log,
        as_of,
        os.getenv("TRADE_INPUT", "final30"),
        os.getenv("MIN_BUYABLE", "5"),
        os.getenv("RELAX_PASSES", "3"),
        os.getenv("MINERVINI_RS_MIN_PCTILE", "80"),
        os.getenv("MINERVINI_VCP_MIN_SCORE", "70"),
    )

    logger.info(
        "[PB1][RUN-START] event=%s now_kst=%s trading_day=%s market_window=%s window=%s phase=%s phase_reason=%s DRY_RUN=%s DISABLE_LIVE_TRADING=%s LIVE_TRADING_ENABLED=%s STRATEGY_MODE=%s PB1_ENTRY_ENABLED=%s",
        event_name_lower or "unknown",
        now.isoformat(),
        trading_day,
        market_window,
        window_label,
        phase_for_log,
        phase_reason,
        dry_run,
        os.getenv("DISABLE_LIVE_TRADING"),
        os.getenv("LIVE_TRADING_ENABLED"),
        os.getenv("STRATEGY_MODE"),
        os.getenv("PB1_ENTRY_ENABLED"),
    )
    execution_mode_label = "trade_intraday_prevclose" if market_window == "after" else f"trade_{window_label}"
    logger.info(
        "[PB1][JOB_CONTEXT] market_window=%s execution_mode=%s",
        market_window,
        execution_mode_label,
    )
    
    # Live Gate 상태 로깅 (시간 기반 자동 정책)
    postprocess_stage = "engine_run"
    try:
        from trader.config import LIVE_GATE_STATUS
        logger.info(
            "[LIVE_GATE_STATUS] allow_live_gate=%s force_block_live=%s reason=%s trading_day=%s window=%s now_kst=%s",
            int(LIVE_GATE_STATUS.allow_live_gate),
            int(LIVE_GATE_STATUS.force_block_live),
            LIVE_GATE_STATUS.reason,
            int(LIVE_GATE_STATUS.trading_day),
            LIVE_GATE_STATUS.window,
            LIVE_GATE_STATUS.now_kst.isoformat(),
        )
    except Exception as e:
        logger.warning("[LIVE_GATE_STATUS] failed to log: %s", e)

    # Ensure defined for all branches (prevents NameError in non-trading-day path)
    dry_run_reason = "unknown"
    engine_started = False
    engine_completed = False
    orders_accepted_count = 0

    if non_trading_day:
        dry_run_reason = "nontrading_day"
        logger.info("[PB1][SKIP] non-trading-day(%s) → diagnostics/dry-run reason=%s", now.date(), dry_run_reason)
        if diag_enabled:
            logger.warning("[PB1][DIAG] non-trading-day(%s) but running diagnostics", now.date())

    runs_repo = RunsRepo(engine)
    run_id = os.getenv("TRADER_RUN_ID", "local")
    # Ensure run row exists early to prevent FK errors
    runs_repo.upsert_run(
        run_id=run_id,
        env=ctx.env,
        strategy=ctx.strategy,
        workflow_run_id=str(ctx.gh_run_number) if ctx.gh_run_number else None,
        ts_start=ctx.started_at,
    )
    universe_repo = UniverseRepo(engine)
    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    positions_repo = PositionsRepo(engine)
    ledger_repo = LedgerEventsRepo(engine)

    run_record_id = None
    engine_runner: PB1Engine | None = None
    did_work = False
    touched_files: list[Path] = []
    result = None
    db_write_reasons: list[str] = []
    universe_ctx: UniverseContext | None = None
    precomputed_final30_df = pd.DataFrame()
    precomputed_derived_df = pd.DataFrame()
    precomputed_universe_df = pd.DataFrame()
    trade_use_precomputed_features = False
    try:
        injected_final30_df = pd.DataFrame()
        if str(os.getenv("MARKET") or os.getenv("PB1_MARKET_SCOPE") or "").upper() in {"KR", "KRX"} or str(os.getenv("PB1_SESSION") or "") in {"am", "afternoon"}:
            injected_final30_df = _load_kr_injected_final30_df(trade_date=trade_date.isoformat(), expected_as_of=str(run_ctx.get("derived_as_of") or as_of), env=env_effective, engine=engine)
            if not injected_final30_df.empty:
                precomputed_final30_df = injected_final30_df.copy()
                final30_context = dict(getattr(injected_final30_df, "attrs", {}).get("final30_context") or {})
                final30_source = str(final30_context.get("source") or "unknown")
                run_ctx["final30_source"] = final30_source
                run_ctx["final30_contract_hash"] = final30_context.get("contract_hash")
                run_ctx["final30_locked"] = True
                run_ctx["final30_rows"] = int(final30_context.get("rows") or len(precomputed_final30_df))
                run_ctx["final30_as_of"] = str(run_ctx.get("derived_as_of") or as_of)
                run_ctx["final30_context"] = final30_context
                setattr(ctx, "final30_df", precomputed_final30_df.copy())
                setattr(ctx, "canonical_source", final30_source)
                setattr(ctx, "final30_contract_hash", final30_context.get("contract_hash"))
                setattr(ctx, "canonical_final30_rows", int(run_ctx["final30_rows"]))
                logger.info("[KR_FINAL30][CONTEXT] source=%s rows=%s hash=%s", final30_source, run_ctx["final30_rows"], run_ctx.get("final30_contract_hash"))
                logger.info("[RUN_ONCE][FINAL30_INJECTED] rows=%s source=%s usable=1 locked=1 scored=1", len(precomputed_final30_df), final30_source)
        final30_strategy_key = os.getenv("PB1_FINAL30_STRATEGY_KEY", "pb1_watchlist_final_scored")
        universe_strategy_key = os.getenv("PB1_UNIVERSE_STRATEGY_KEY", os.getenv("PB1_UNIVERSE_STRATEGY", DEFAULT_UNIVERSE_STRATEGY))
        candidate_pool_strategy_key = os.getenv("PB1_CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool")
        logger.info("[STRATEGY_KEY][LOCK] final30=%s universe=%s candidate_pool=%s", final30_strategy_key, universe_strategy_key, candidate_pool_strategy_key)
        should_lock_entry_sources = (
            not close_cancel_only
            and (
                trading_day
                or compute_only_full_run
                or (not trading_day)
            )
            and (
                market_window in {"preopen", "morning", "day", "close", "after", "intraday"}
                or compute_only_full_run
                or (not trading_day)
            )
        )
        if should_lock_entry_sources:
            universe_strategy = os.getenv("PB1_UNIVERSE_STRATEGY_KEY") or os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
            try:
                universe_ctx = _load_universe_context(
                    engine=engine,
                    as_of=str(run_ctx.get("derived_as_of") or as_of),
                    env=env_effective,
                    strategy=universe_strategy,
                )
            except RuntimeError as exc:
                logger.error("[PB1][UNIVERSE][FAIL] as_of=%s err=%s", as_of, exc)
                raise
            if getattr(universe_ctx, "is_empty", False):
                logger.info(
                    "[PB1][SKIP] empty universe -> no candidates today; phase=%s window=%s",
                    phase_for_log,
                    window_label,
                )
                return touched_files, False, {}, phase_for_log, "SKIP_EMPTY_UNIVERSE"
            mode_input = (os.getenv("MODE") or "").strip().lower()
            if mode_input == "trade" and universe_ctx and universe_ctx.members:
                locked_rows = list((universe_ctx.meta or {}).get("locked_final30_rows") or [])
                if precomputed_final30_df.empty:
                    precomputed_final30_df = pd.DataFrame(locked_rows)
                if not precomputed_final30_df.empty:
                    final30_source_name = str(run_ctx.get("final30_source") or (universe_ctx.meta or {}).get("source") or "db_pb1_watchlist_final_scored")
                    run_ctx["final30_source"] = final30_source_name
                    run_ctx["final30_locked"] = True
                    run_ctx["final30_rows"] = int(len(precomputed_final30_df))
                    run_ctx["final30_as_of"] = str(run_ctx.get("derived_as_of") or as_of)
                    run_ctx["final30_codes"] = [
                        str(code).zfill(6)
                        for code in precomputed_final30_df.get("code", pd.Series(dtype=str)).tolist()
                        if str(code).strip()
                    ]
                    trade_use_precomputed_features = True
                    logger.info(
                        "[ASOF][LOCK][VERIFY] trade_date=%s derived_as_of=%s final30_source='%s' final30_locked=%s",
                        trade_date.isoformat(),
                        str(run_ctx.get("derived_as_of") or as_of),
                        final30_source_name,
                        bool(run_ctx.get("final30_locked")),
                    )
                    logger.info(
                        "[TRADE][FINAL30][DB_LOCK] env=%s as_of=%s rows=%s scored=%s immutable=1",
                        env_effective,
                        str(run_ctx.get("derived_as_of") or as_of),
                        len(precomputed_final30_df),
                        int(final30_source_name == "db_pb1_watchlist_final_scored"),
                    )
                    logger.info(
                        "[TRADE][FINAL30][DB_EXACT_LOAD] env=%s as_of=%s rows=%s uniq_codes=%s uniq_ranks=%s",
                        env_effective,
                        str(run_ctx.get("derived_as_of") or as_of),
                        len(precomputed_final30_df),
                        precomputed_final30_df.get("code", pd.Series(dtype=str)).astype(str).str.zfill(6).nunique(),
                        precomputed_final30_df.get("rank_final30", pd.Series(dtype=float)).nunique() if "rank_final30" in precomputed_final30_df.columns else 0,
                    )
                    logger.info(
                        "[TRADE][FINAL30][LOCKED_DF] rows=%s cols=%s source=%s",
                        len(precomputed_final30_df),
                        list(precomputed_final30_df.columns),
                        run_ctx["final30_source"],
                    )
                    logger.info(
                        "[TRADE][PRECOMPUTED_FEATURES] enabled=1 source=pb1_watchlist_final_scored rows=%s",
                        len(precomputed_final30_df),
                    )
                    symbols = [str(x).zfill(6) for x in precomputed_final30_df.get("code", pd.Series(dtype=str)).tolist() if str(x).strip()]
                    try:
                        derived_repo = DerivedMinerviniRepo(engine)
                        derived_rows, _actual_as_of = derived_repo.load_for_as_of_with_fallback(
                            env=env_effective,
                            as_of=date.fromisoformat(str(run_ctx.get("derived_as_of") or as_of)),
                            symbols=symbols,
                            ttl_days=7,
                        )
                        precomputed_derived_df = pd.DataFrame(list(derived_rows or []))
                    except Exception as exc:
                        logger.warning("[TRADE][PRECOMPUTED_FEATURES][DERIVED_LOAD_FAIL] err=%s", exc)
                    try:
                        watchlist_repo = WatchlistRepo(engine)
                        universe_rows, _used_as_of = watchlist_repo.load_watchlist_scored(
                            env=env_effective,
                            strategy="pb1_universe_scored",
                            as_of=date.fromisoformat(str(run_ctx.get("derived_as_of") or as_of)),
                            allow_latest_fallback=True,
                            ttl_days=7,
                            max_back_days=3,
                        )
                        precomputed_universe_df = pd.DataFrame(list(universe_rows or []))
                        actual_source = "pb1_universe_scored" if not precomputed_universe_df.empty else "none"
                        fallback_used = int(
                            bool(
                                not precomputed_universe_df.empty
                                and str(_used_as_of or "") != str(run_ctx.get("derived_as_of") or as_of)
                            )
                        )
                        contract_ok = int(not precomputed_universe_df.empty)
                        log_fn = logger.info if contract_ok else logger.warning
                        log_fn(
                            "[DB][PRECOMPUTED_FEATURES][LOAD_RESULT] requested_strategy=%s actual_source=%s fallback_used=%s rows=%s contract_ok=%s critical=0",
                            "pb1_universe_scored",
                            actual_source,
                            fallback_used,
                            len(precomputed_universe_df),
                            contract_ok,
                        )
                    except Exception:
                        precomputed_universe_df = pd.DataFrame()
                        logger.warning(
                            "[DB][PRECOMPUTED_FEATURES][LOAD_RESULT] requested_strategy=%s actual_source=none fallback_used=0 rows=0 contract_ok=0 critical=0",
                            "pb1_universe_scored",
                        )
                    logger.info(
                        "[ENTRY_SOURCE][LOCK] final30_rows=%s watchlist_rows=%s precomputed_rows=%s",
                        len(precomputed_final30_df),
                        len(precomputed_final30_df),
                        len(precomputed_derived_df),
                    )
            setattr(ctx, "precomputed_final30_df", precomputed_final30_df)
            setattr(ctx, "precomputed_derived_df", precomputed_derived_df)
            setattr(ctx, "precomputed_universe_df", precomputed_universe_df)
            setattr(ctx, "trade_use_precomputed_features", bool(trade_use_precomputed_features))
        phase_name_for_lock = str(run_ctx.get("phase_name") or phase_override_arg or os.getenv("FORCE_PB1_PHASE") or "").strip().lower()
        session_kind_for_lock = str(run_ctx.get("session_kind") or os.getenv("PB1_SESSION_KIND") or "").strip().lower()
        is_close_or_exit_only_for_lock = _is_close_or_exit_only_session(
            phase_name=phase_name_for_lock,
            session_kind=session_kind_for_lock,
        )
        if (os.getenv("MODE") or "").strip().lower() == "trade":
            _validate_trade_locked_final30_or_raise(
                final30_df=precomputed_final30_df,
                source_name=str(run_ctx.get("final30_source") or "none"),
                as_of=str(run_ctx.get("derived_as_of") or as_of or ""),
                env=str(run_ctx.get("env") or os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice"),
                allow_empty_for_close_exit=is_close_or_exit_only_for_lock,
            )
        if _resolve_session_kind(now) == "afternoon":
            final30_rows_for_mode = int(len(precomputed_final30_df) if isinstance(precomputed_final30_df, pd.DataFrame) else 0)
            db_prep_done_for_mode = 0
            try:
                db_prep_done_for_mode = int(bool(ledger_repo.get_prep_done_event(env=env_effective, as_of=str(run_ctx.get("derived_as_of") or as_of))))
            except Exception as exc:
                logger.warning("[TRADE_AFTERNOON][PREP_DONE_CHECK][WARN] err=%s", exc)
            shell_prep_ready = str(os.getenv("SHELL_PREP_READY") or os.getenv("PREP_READY") or "").strip()
            contract_ok_for_mode = int(final30_rows_for_mode > 0 and bool(run_ctx.get("final30_locked")))
            now_hhmm = now.strftime("%H:%M")
            is_market_open = "09:00" <= now_hhmm < "15:20"
            is_entry_window = "09:00" <= now_hhmm < "15:10"
            is_close_window = now_hhmm >= "15:10"
            if shell_prep_ready == "0" and db_prep_done_for_mode == 1 and final30_rows_for_mode > 0 and contract_ok_for_mode:
                logger.warning(
                    "[TRADE_AFTERNOON][PREP_READY_OVERRIDE] shell_prep_ready=0 db_prep_done=1 final30_rows=%s contract_ok=1 action=enable_entry",
                    final30_rows_for_mode,
                )
            elif db_prep_done_for_mode == 1 and final30_rows_for_mode > 0 and contract_ok_for_mode:
                logger.info(
                    "[TRADE_AFTERNOON][PREP_READY_DB_OK] db_prep_done=1 final30_rows=%s contract_ok=1 action=enable_entry",
                    final30_rows_for_mode,
                )
            mode_entry_enabled = bool(is_market_open and is_entry_window and db_prep_done_for_mode == 1 and final30_rows_for_mode > 0)
            mode_exit_only = bool((not mode_entry_enabled) and (is_close_window or not is_market_open or final30_rows_for_mode == 0))
            mode_reason = "entry_window" if mode_entry_enabled else ("close_window" if is_close_window else "market_closed_or_prep_missing")
            logger.info(
                "[TRADE_AFTERNOON][MODE_DECISION] now=%s market_open=%s entry_window=%s close_window=%s db_prep_done=%s final30_rows=%s entry_enabled=%s exit_only=%s reason=%s",
                now.isoformat(),
                int(is_market_open),
                int(is_entry_window),
                int(is_close_window),
                db_prep_done_for_mode,
                final30_rows_for_mode,
                int(mode_entry_enabled),
                int(mode_exit_only),
                mode_reason,
            )
            if mode_entry_enabled:
                _applied_mode = _apply_afternoon_mode_decision(run_ctx=run_ctx, entry_enabled=True, exit_only=False)
                entry_flag = parse_env_flag("PB1_ENTRY_ENABLED", default=True)
                resolved_phase = str(_applied_mode["phase"])
                phase_override_arg = str(_applied_mode["phase"])
                phase_for_log = str(_applied_mode["phase"])
                phase_reason = "db_prep_final30"
            elif mode_exit_only:
                _apply_afternoon_mode_decision(run_ctx=run_ctx, entry_enabled=False, exit_only=True)
        kis: KisAPI | None = None
        allow_compute_without_kis = bool(
            compute_only_full_run
            or (not trading_day)
            or market_window == "after"
            or bool(compute_only_flags.get("force_block_live"))
        )
        try:
            if manual_test_route:
                os.environ["KIS_HTTP_CALLER_ROUTE"] = "manual_test"
            elif mode == "LIVE":
                os.environ["KIS_HTTP_CALLER_ROUTE"] = "live"
            else:
                os.environ["KIS_HTTP_CALLER_ROUTE"] = "trade"
            kis = KisAPI()
            if kis.env != env_effective:
                logger.warning(
                    "[PB1][KIS_ENV_MISMATCH] kis.env=%s != effective_env=%s strategy_env=%s kis_env=%s -> force dry_run + downgrade intended_live",
                    kis.env,
                    env_effective,
                    env_strategy,
                    env_kis,
                )
                dry_run = True
                intended_live = False  # ✅ CRITICAL: must sync intended_live when forcing dry_run
                _apply_env_flags_if_needed(dry_run)
        except Exception:
            if allow_compute_without_kis:
                logger.warning("[PB1][KIS][OPTIONAL_FAIL] init failed but compute-only path continues", exc_info=True)
                kis = None
            else:
                logger.exception("[PB1] KIS init failed -> skip tick")
                return touched_files, False, {}, phase_for_log, "SKIP_KIS_INIT"

        balance_snapshot_raw: dict | None = None
        balance_source: str | None = None
        balance_state = BALANCE_STATE_UNKNOWN
        precheck_path = os.getenv("KR_BALANCE_PRECHECK_PATH", "").strip()
        if precheck_path and Path(precheck_path).exists():
            try:
                pre = json.loads(Path(precheck_path).read_text(encoding="utf-8"))
                state = str(pre.get("state") or "UNKNOWN").upper()
                snapshot = pre.get("raw_snapshot") if isinstance(pre.get("raw_snapshot"), dict) else None
                if state == "OK" and snapshot:
                    balance_state = BALANCE_STATE_OK
                    balance_snapshot_raw = snapshot
                elif state == "OK":
                    state = "UNKNOWN_WITHOUT_SNAPSHOT"
                    balance_state = BALANCE_STATE_UNKNOWN
                else:
                    balance_state = BALANCE_STATE_UNKNOWN
                balance_source = str(pre.get("source") or "NONE")
                logger.info("[PB1][BALANCE_PRECHECK_USE] state=%s source=%s requery=0 snapshot=%d", state, balance_source, int(bool(balance_snapshot_raw)))
                if state != "OK":
                    logger.warning("[PB1][ENTRY_BLOCKED] reason=balance_unknown")
                    if bool(pre.get("exit_allowed")):
                        logger.warning("[PB1][EXIT_CONTINUE] reason=balance_unknown_exit_allowed")
                    if bool(pre.get("close_allowed")):
                        logger.warning("[PB1][CLOSE_CONTINUE] reason=balance_unknown_close_allowed")
            except Exception as exc:
                logger.warning("[PB1][BALANCE_PRECHECK_USE][FAIL] err=%s", exc)
        elif kis:
            _log_balance_cache(force=False)
            balance_state, balance_snapshot_raw, balance_source = get_balance_state(
                kis=kis,
                now=now,
            )
        logger.info(
            "[PB1][BALANCE][STATE] state=%s source=%s",
            balance_state,
            balance_source,
        )
        account_sanity = _practice_account_sanity_check(
            env=env_effective,
            kis=kis,
            balance_state=balance_state,
            balance_snapshot=balance_snapshot_raw,
        )
        account_sanity_block_reason = None
        if account_sanity.get("enabled"):
            logger.info(
                "[ACCOUNT_SANITY][CHECK] account=%s ok=%s holdings=%s expected_holdings=%s cash=%s expected_capital=%s tolerance=%s reason=%s",
                account_sanity.get("masked_account"),
                int(bool(account_sanity.get("ok"))),
                account_sanity.get("holdings_count"),
                account_sanity.get("expected_holdings"),
                account_sanity.get("cash_krw"),
                account_sanity.get("expected_capital_krw"),
                account_sanity.get("capital_tolerance_krw"),
                account_sanity.get("reason"),
            )
            if not account_sanity.get("ok") and account_env_flag("ACCOUNT_SANITY_FAIL_IF_MISMATCH", default=True):
                account_sanity_block_reason = f"account_sanity_fail:{account_sanity.get('reason')}"
                logger.error(
                    "[ACCOUNT_SANITY][FAIL] account=%s reason=%s",
                    account_sanity.get("masked_account"),
                    account_sanity.get("reason"),
                )
        _maybe_reconcile_practice_account_state(
            engine=engine,
            env=env_effective,
            kis=kis,
            balance_state=balance_state,
            balance_snapshot=balance_snapshot_raw,
            positions_repo=positions_repo,
        )
        if balance_state == BALANCE_STATE_STALE_OK:
            logger.warning("[PB1][BALANCE][STALE_OK] using recent snapshot for exits")

        # ✅ [GATE SEPARATION] calc_allowed, price_allowed, order_allowed
        minervini_only = os.getenv("MINERVINI_ONLY", "0") == "1"
        mode_input = (os.getenv("MODE") or "").strip().lower()
        minervini_test = mode_input == "minervini_test"
        
        # calc_allowed: 분석/스코어 계산 가능 여부
        if minervini_only:
            calc_allowed = True  # MINERVINI_ONLY는 시간 무관하게 계산만 수행
            logger.info("[PB1][MINERVINI_ONLY] calc_allowed=1 (cutoff/window ignored)")
        else:
            calc_allowed = True  # 기본적으로 계산은 항상 허용
        
        # price_allowed: 가격 조회 가능 여부 (MINERVINI_ONLY에서는 DB만 사용)
        price_allowed = True  # Minervini는 OHLCV 종가 기반이므로 HTTP 필요 없음
        
        # order_allowed: 주문 생성/제출 가능 여부
        user_entry_enabled = bool(entry_flag.value)
        order_allowed = user_entry_enabled and not minervini_only
        entry_block_reason = None
        os.environ["PB1_PM_POLICY_REASON"] = ""
        
        # MINERVINI_ONLY 강제 설정
        if minervini_only:
            order_allowed = False
            entry_block_reason = "minervini_only_mode"
            logger.info("[PB1][MINERVINI_ONLY] order_allowed=0 KIS_HTTP_ENABLED=%s DRY_RUN=%s",
                       os.getenv("KIS_HTTP_ENABLED", "N/A"), os.getenv("DRY_RUN", "N/A"))

        if minervini_test:
            calc_allowed = True
            order_allowed = False
            entry_block_reason = entry_block_reason or "minervini_test"
            logger.info("[PB1][MINERVINI_TEST] compute_allowed=1 order_allowed=0")
        
        # [2] 거래시간 체크: LIVE 모드에서 장중 여부 판정
        if mode == "LIVE" and not minervini_only:
            is_weekday = now.weekday() < 5  # Mon-Fri
            market_open = datetime.strptime("09:00", "%H:%M").time()
            market_close = datetime.strptime("15:20", "%H:%M").time()
            in_market_hours = is_weekday and (market_open <= now.time() < market_close)
            if not in_market_hours:
                logger.info("[PB1][LIVE][OUT_OF_MARKET] now=%s weekday=%s -> no entry/exit", now.isoformat(), is_weekday)
                order_allowed = False
                entry_block_reason = entry_block_reason or "out_of_market_hours"
        
        if not user_entry_enabled and not minervini_only:
            order_allowed = False
            entry_block_reason = "entry_disabled"
        session_policy = _resolve_session_trade_policy(
            session=_resolve_session_kind(now),
            phase_name=resolved_phase,
            entry_enabled=user_entry_enabled,
            now=now,
        )
        session_warning_reason = str(session_policy.get("reason") or "")
        if session_warning_reason == "LATE_START_BEFORE_CUTOFF":
            logger.info("[TRADE_PM][POLICY] warning_only=1 reason=%s", session_warning_reason)
        if session_policy["no_new_entry"] and not minervini_only:
            logger.info("[TRADE_PM][POLICY] no_new_entry=1 reason=%s", session_policy["reason"])
            os.environ["PB1_PM_POLICY_REASON"] = str(session_policy["reason"])
            order_allowed = False
            entry_block_reason = entry_block_reason or str(session_policy["reason"])
            if str(session_policy["reason"] or "") == "ENTRY_CUTOFF_PASSED":
                os.environ["PB1_ENTRY_ENABLED"] = "0"
                os.environ["ALLOW_NEW_BUY"] = "0"
                os.environ["ENTRY_ENABLED"] = "0"
                os.environ["FORCE_ENTRY_DISABLED_REASON"] = "ENTRY_CUTOFF_PASSED"
                os.environ["PB1_PHASE_DEFAULT"] = "exit"
                phase_override_arg = "exit"
                phase_for_log = "exit"
                run_ctx["phase_name"] = "exit"
                logger.info(
                    "[ENTRY][DISABLED] reason=ENTRY_CUTOFF_PASSED action=skip_entry_scan phase=exit order_allowed=0"
                )
                logger.info("[EXIT][ENABLED] reason=entry_cutoff_exit_only")
        if account_sanity_block_reason and not minervini_only:
            dry_run = True
            intended_live = False
            _apply_env_flags_if_needed(dry_run)
            order_allowed = False
            entry_block_reason = entry_block_reason or account_sanity_block_reason
            logger.warning(
                "[RUN_SUMMARY][WARN] reason=account_sanity_fail account=%s detail=%s",
                account_sanity.get("masked_account"),
                account_sanity.get("reason"),
            )
        if balance_state == BALANCE_STATE_UNKNOWN and not minervini_only:
            order_allowed = False
            entry_block_reason = entry_block_reason or "balance_unknown"
        elif PB1_REQUIRE_BALANCE_FOR_ENTRY and balance_state == BALANCE_STATE_STALE_OK and not minervini_only:
            order_allowed = False
            entry_block_reason = entry_block_reason or "balance_stale"
        if window_label not in {"preopen", "morning", "day"} and not minervini_only and not force_entry_window_override:
            order_allowed = False
            entry_block_reason = entry_block_reason or "window_blocked"
        entry_cutoff_raw = (os.getenv("ENTRY_CUTOFF_TIME") or PB1_ENTRY_WINDOW_END or "").strip()
        if entry_cutoff_raw and not minervini_only:
            try:
                cutoff_time = datetime.strptime(entry_cutoff_raw, "%H:%M").time()
                cutoff_dt = datetime.combine(now.date(), cutoff_time, tzinfo=now.tzinfo)
                logger.info(
                    "[PB1][TIME] now_kst=%s cutoff=%s close=%s phase=%s",
                    now.isoformat(),
                    cutoff_dt.isoformat(),
                    close_dt.isoformat(),
                    resolved_phase,
                )
                if now.time() >= cutoff_time:
                    force_compute_when_cutoff = (
                        env_bool("FORCE_COMPUTE_ON_CUTOFF", False)
                        or env_bool("BYPASS_ENTRY_CUTOFF_COMPUTE_ONLY", False)
                        or env_bool("FORCE_COMPUTE_WHEN_CUTOFF", False)
                        or env_bool("BYPASS_ENTRY_CUTOFF_FOR_COMPUTE", False)
                        or PB1_DIAG_IGNORE_ENTRY_CUTOFF
                    )
                    order_allowed = False
                    entry_block_reason = entry_block_reason or "ENTRY_CUTOFF_PASSED"
                    if force_compute_when_cutoff:
                        calc_allowed = True
                        logger.info(
                            "[PB1][CUTOFF_OVERRIDE] compute_only=1 order_allowed=0 (reason=%s)",
                            entry_block_reason,
                        )
            except ValueError:
                logger.warning("[PB1][ENV] invalid ENTRY_CUTOFF_TIME=%s", entry_cutoff_raw)

        if market_window == "preopen" and not minervini_only:
            if not PB1_ALLOW_PREOPEN_ENTRY:
                order_allowed = False
                entry_block_reason = entry_block_reason or "preopen_block"
            elif PB1_PREOPEN_REQUIRE_BALANCE and balance_state != BALANCE_STATE_OK:
                order_allowed = False
                entry_block_reason = entry_block_reason or "balance_unknown"

        if deadline_ts and not minervini_only:
            remaining_budget = deadline_ts - time_mod.monotonic()
            if remaining_budget <= persist_budget_sec:
                order_allowed = False
                entry_block_reason = entry_block_reason or "timeout_budget"
                logger.warning(
                    "[PB1][TIMEOUT][ENTRY_BLOCKED] remaining=%.1fs persist_budget=%s trade_budget=%s",
                    remaining_budget,
                    persist_budget_sec,
                    trade_budget_sec,
                )

        if kis and getattr(kis, "safe_mode", False) and not minervini_only:
            order_allowed = False
            entry_block_reason = entry_block_reason or "safe_mode"
            logger.warning("[PB1][SAFE_MODE] order_allowed=0")
            try:
                ReconcileLogRepo(engine).append_log(
                    env=env_effective,
                    strategy="pb1_pullback_close",
                    tick_ts=now,
                    action="safe_mode_entry_block",
                    details_json={"reason": "safe_mode"},
                )
            except Exception:
                logger.warning("[PB1][SAFE_MODE][RECONCILE_LOG][FAIL]", exc_info=True)

        # ✅ 게이트 요약 로그
        logger.info(
            "[PB1][GATE] calc_allowed=%s price_allowed=%s order_allowed=%s minervini_only=%s reason=%s",
            int(calc_allowed), int(price_allowed), int(order_allowed), int(minervini_only),
            entry_block_reason or "none"
        )
        
        if not calc_allowed:
            logger.info(
                "[PB1][CALC_BLOCKED] reason=%s -> skip analytics",
                entry_block_reason or "unknown",
            )
            # 계산도 못하면 조기 종료
            return [], False, {}, phase_for_log, "SKIP_ANALYTICS"
        
        if not order_allowed:
            logger.info(
                "[PB1][ORDER_BLOCKED] reason=%s order_allowed=0",
                entry_block_reason or "unknown",
            )

        if after_close_entry_dryrun:
            calc_allowed = True
            order_allowed = False
            entry_block_reason = "after_close_entry_dryrun"
            logger.warning(
                "[PB1][AFTER_CLOSE_ENTRY_DRYRUN][COMPUTE_ONLY] calc_allowed=%s price_allowed=%s order_allowed=%s reason=%s",
                int(calc_allowed),
                int(price_allowed),
                int(order_allowed),
                entry_block_reason,
            )

        nontrading_eval_mode = _is_nontrading_eval_mode(
            trading_day=trading_day,
            strategy_mode=mode,
            force_block_live=bool(compute_only_flags.get("force_block_live")),
            order_allowed=order_allowed,
        )
        if nontrading_eval_mode:
            os.environ["NONTRADING_EVAL_MODE"] = "1"
            logger.info(
                "[NONTRADING_EVAL][MODE] enabled=1 trading_day=%s strategy_mode=%s force_block_live=%s order_allowed=%s",
                int(bool(trading_day)),
                mode,
                int(bool(compute_only_flags.get("force_block_live"))),
                int(bool(order_allowed)),
            )

        run_record_id = runs_repo.start_run(
            env=env_effective,
            strategy="pb1_pullback_close",
            run_window=window_label,
            phase=phase_override_arg,
            event_name=event_name_lower,
            dry_run=dry_run,
            git_sha=os.getenv("GITHUB_SHA"),
            workflow=os.getenv("GITHUB_WORKFLOW"),
            workflow_run_id=workflow_run_id,
            workflow_attempt=int(os.getenv("GITHUB_RUN_ATTEMPT", "0") or 0),
            config_json={
                "run_window": window_label,
                "phase": phase_override_arg,
                "phase_reason": phase_reason,
                "intended_live": intended_live,
                "dry_run": dry_run,
            },
        )
        db_write_reasons.append("run_start")
        reconcile_result: dict | None = None
        if kis and not nontrading_eval_mode:
            try:
                reconcile_result = reconcile_kis(
                    engine=engine,
                    kis=kis,
                    env=env_effective,
                    run_id=run_record_id,
                    strategy="pb1_pullback_close",
                    tick_ts=now,
                    balance_snapshot=balance_snapshot_raw,
                    runtime_dir=str(runtime_root_dir),
                )
                db_write_reasons.append("reconcile")
                if not reconcile_result.get("ok", True):
                    logger.warning(
                        "[PB1][RECONCILE][DEGRADED] reason=%s err=%s",
                        reconcile_result.get("reason"),
                        reconcile_result.get("err"),
                    )
            except KisTemporaryError as exc:
                logger.warning("[PB1][RECONCILE][DEGRADED] %s", exc)
            except Exception as exc:
                if not dry_run and mode_resolved == "LIVE":
                    logger.error("[PB1][RECONCILE][FAIL] %s", exc)
                logger.warning("[PB1][RECONCILE][WARN] %s", exc)
        elif nontrading_eval_mode:
            logger.info("[NONTRADING_EVAL][RECONCILE][SKIP] reason=read_only_mode")

        # ========== Stale open order 자동 정리 ==========
        expire_stale_enabled = str(os.getenv("PB1_EXPIRE_STALE_OPEN_ORDERS") or "1") == "1"
        stale_max_minutes = int(os.getenv("PB1_STALE_OPEN_ORDER_MAX_MINUTES") or "30")
        stale_repair_practice = str(os.getenv("PB1_STALE_OPEN_ORDER_REPAIR_PRACTICE") or "1") == "1"
        
        if expire_stale_enabled and (env_effective == "practice" and stale_repair_practice or env_effective == "real"):
            try:
                cutoff_dt = now - timedelta(minutes=stale_max_minutes)
                logger.info(
                    "[ORDERS][STALE_REPAIR][START] env=%s cutoff=%s max_minutes=%s",
                    env_effective,
                    cutoff_dt.isoformat(),
                    stale_max_minutes,
                )
                expired_count = orders_repo.expire_stale_open_orders(
                    env=env_effective,
                    before_dt=cutoff_dt,
                    reason="STALE_OPEN_ORDER_EXPIRED",
                )
                remaining_open = len(orders_repo.get_open_orders(env_effective, include_stale=False) or [])
                logger.info(
                    "[ORDERS][STALE_REPAIR][DONE] env=%s expired=%s remaining_open=%s",
                    env_effective,
                    expired_count,
                    remaining_open,
                )
                if expired_count > 0:
                    db_write_reasons.append("stale_order_cleanup")
            except Exception as exc:
                logger.warning("[ORDERS][STALE_REPAIR][FAIL] env=%s err=%s", env_effective, exc)

        reconcile_only_enabled = str(os.getenv("PB1_RECONCILE_ONLY_AFTER_ORDER_SUBMIT") or "0") == "1"
        reconcile_only_pending = str(os.getenv("PB1_PENDING_RECONCILE_ONLY") or "0") == "1"
        reconcile_only_global_skip = str(os.getenv("PB1_RECONCILE_ONLY_GLOBAL_SKIP") or "0") == "1"
        force_reconcile_only = str(os.getenv("PB1_FORCE_RECONCILE_ONLY") or "0") == "1"
        
        if reconcile_only_enabled and reconcile_only_pending:
            open_orders_count = 0
            try:
                open_orders_count = len(orders_repo.get_open_orders(env_effective) or [])
            except Exception as exc:
                logger.warning("[PB1][RECONCILE_ONLY][OPEN_ORDERS_READ_FAIL] err=%s", exc)
            
            skip_engine, keep_pending = _resolve_reconcile_only_followup(
                enabled=True,
                pending=True,
                buy_orders=0,
                sell_orders=0,
                open_orders_count=open_orders_count,
            )
            
            if keep_pending:
                os.environ["PB1_PENDING_RECONCILE_ONLY"] = "1"
            else:
                os.environ.pop("PB1_PENDING_RECONCILE_ONLY", None)
            
            # ========== open order 존재 → 전체 엔진 skip 방지 ==========
            # reconcile_only_global_skip=0 (기본값)이면 open order가 있어도 전체 엔진 계속 실행
            # 종목별 open order 체크는 entry candidate 평가 시 개별 처리
            if skip_engine and not reconcile_only_global_skip and not force_reconcile_only:
                logger.info(
                    "[PB1][RECONCILE_ONLY][BYPASS] open_orders=%s global_skip=0 action=continue_engine reason=code_scoped_order_guard",
                    open_orders_count,
                )
                skip_engine = False
            
            if skip_engine:
                logger.info(
                    "[PB1][RECONCILE_ONLY] enabled=1 pending=1 open_orders=%s global_skip=%s action=skip_engine keep_pending=%s",
                    open_orders_count,
                    int(reconcile_only_global_skip or force_reconcile_only),
                    int(keep_pending),
                )
                runs_repo.finish_run(run_record_id, status="OK_NO_TRADE", notes="reconcile_only_after_submit")
                db_write_reasons.append("reconcile_only")
                _write_last_db_write(runtime_root_dir, run_id=str(run_record_id), reason="reconcile_only", now=now)
                return [], True, {"buy_orders": 0, "sell_orders": 0, "warning_counts": {}}, phase_for_log, "OK_RECONCILE_ONLY"
            else:
                logger.info(
                    "[PB1][RECONCILE_ONLY][CHECK] open_orders=%s global_skip=0 action=continue",
                    open_orders_count,
                )

        logger.info(
            "[TRADE][PRECHECK][BALANCE] state=%s require_balance=%s allow_compute_without_kis=%s",
            balance_state,
            int(bool(PB1_REQUIRE_BALANCE_FOR_ENTRY)),
            int(bool(allow_compute_without_kis)),
        )
        continue_after_balance_precheck, order_allowed, entry_block_reason, balance_precheck_return_reason = _handle_balance_unknown_precheck(
            balance_state=balance_state,
            require_balance_for_entry=PB1_REQUIRE_BALANCE_FOR_ENTRY,
            allow_compute_without_kis=allow_compute_without_kis,
            order_allowed=order_allowed,
            entry_block_reason=entry_block_reason,
        )
        if not continue_after_balance_precheck:
            logger.error("[TRADE][READY][FAIL] reason=balance_precheck_failed")
            logger.warning("[PB1][DEGRADED] reason=balance_unknown -> skip trading")
            runs_repo.finish_run(run_record_id, status="DEGRADED", notes="balance_unknown")
            db_write_reasons.append("balance_degraded")
            _write_last_db_write(runtime_root_dir, run_id=str(run_record_id), reason="balance_degraded", now=now)
            return [], False, {}, phase_for_log, balance_precheck_return_reason or "DEGRADED_BALANCE_UNKNOWN"

        # ✅ dry_run은 이미 LIVE_ENV_LOCK에서 파싱 완료 (재파싱 금지)
        # 엔진에 전달할 값 최종 확인: bool 타입 강제
        dry_run_for_engine = parse_bool_any(dry_run, default=True)
        
        logger.info(
            "[ENGINE][INIT] dry_run=%s (input_type=%s, parsed_type=%s) intended_live=%s phase=%s window=%s",
            dry_run_for_engine,
            type(dry_run).__name__,
            type(dry_run_for_engine).__name__,
            intended_live,
            phase_override_arg,
            window_label,
        )

        phase_name_for_engine = str(run_ctx.get("phase_name") or phase_override_arg or "entry").strip().lower() or "entry"
        window_name_for_engine = str(run_ctx.get("window_name") or normalized_window_name or "day").strip().lower() or "day"
        session_kind_for_engine = str(run_ctx.get("session_kind") or os.getenv("PB1_SESSION_KIND") or "").strip().lower()
        precomputed_final30_df = _guard_empty_final30_for_engine_boot(
            precomputed_final30_df,
            phase_name=phase_name_for_engine,
            session_kind=session_kind_for_engine,
        )
        close_liquidation_enabled_for_engine = str(
            os.getenv("PB1_CLOSE_LIQUIDATION_ENABLED", os.getenv("CLOSE_LIQUIDATION_ENABLED", "0"))
        ).strip().lower() not in {"0", "false", "no"}
        if _is_close_or_exit_only_session(phase_name=phase_name_for_engine, session_kind=session_kind_for_engine):
            if close_liquidation_enabled_for_engine:
                holdings_for_liquidation = None
                if isinstance(balance_snapshot_raw, dict):
                    snapshot_holdings = balance_snapshot_raw.get("output1") or []
                    if snapshot_holdings:
                        holdings_for_liquidation = list(snapshot_holdings)
                if holdings_for_liquidation is None:
                    logger.warning("[KR_CLOSE][LIQUIDATION][BALANCE_RELOAD] reason=missing_or_empty_snapshot source=kis_client")
                else:
                    logger.info("[KR_CLOSE][LIQUIDATION][BALANCE_SNAPSHOT] holdings=%s", len(holdings_for_liquidation))
                liquidation_results = run_emergency_close_liquidation_from_kis_holdings(
                    kis_client=kis,
                    orders_repo=orders_repo,
                    env=env_effective,
                    holdings=holdings_for_liquidation,
                    dry_run=dry_run_for_engine,
                )
                logger.info("[KR_CLOSE][LIQUIDATION][DONE] orders=%s", len(liquidation_results))
                return [], bool(liquidation_results), {"buy_orders": 0, "sell_orders": len(liquidation_results), "warning_counts": {}}, phase_for_log, "OK_CLOSE_LIQUIDATION"
            holdings_for_policy = []
            if isinstance(balance_snapshot_raw, dict):
                holdings_for_policy = list(balance_snapshot_raw.get("output1") or balance_snapshot_raw.get("holdings") or [])
            close_policy_result = run_kr_close_policy_from_tagged_positions(
                kis_holdings=holdings_for_policy,
                positions_repo=positions_repo,
                fills_repo=fills_repo,
                orders_repo=orders_repo,
                kis_client=kis,
                env=env_effective,
                strategy="pb1_pullback_close",
                dry_run=dry_run_for_engine,
            )
            policy_orders = list(close_policy_result.get("policy_orders") or [])
            policy_results = list(close_policy_result.get("policy_results") or [])
            accepted_policy_sells = int(close_policy_result.get("accepted_policy_sells") or 0)
            logger.info("[KR_CLOSE][LIQUIDATION][DISABLED]")
            logger.info(
                "[KR_CLOSE][POLICY][DONE] policy_sell_candidates=%s submitted=%s accepted=%s holds=%s",
                len(policy_orders), len(policy_results), accepted_policy_sells, max(0, len(holdings_for_policy) - len(policy_orders)),
            )
            return [], True, {
                "buy_orders": 0,
                "sell_orders": accepted_policy_sells,
                "sell_orders_ack": accepted_policy_sells,
                "policy_sell_candidates": len(policy_orders),
                "warning_counts": {},
            }, phase_for_log, "OK_CLOSE_POLICY"

        logger.info(
            "[RUN_ONCE][ENGINE_ARGS] phase_name=%s window_name=%s market_window=%s phase=%s intended_live=%s",
            phase_name_for_engine,
            window_name_for_engine,
            market_window,
            phase_override_arg,
            intended_live,
        )
        logger.info(
            "[TRADE][ENGINE_BOOT][START] engine=PB1Engine phase_name=%s window_name=%s",
            phase_name_for_engine,
            window_name_for_engine,
        )

        if balance_state == BALANCE_STATE_UNKNOWN and entry_block_reason == "balance_unknown":
            balance_snapshot_raw = {
                "output1": [],
                "output2": [{
                    "dnca_tot_amt": "0",
                    "ord_psbl_cash": "0",
                    "scts_evlu_amt": "0",
                    "tot_evlu_amt": "0",
                }],
                "_source": "balance_fail_soft_empty",
                "_fail_soft": True,
                "_balance_state": str(balance_state),
            }
            balance_source = "balance_fail_soft_empty"
            os.environ["PB1_BALANCE_FAIL_SOFT"] = "1"
            logger.warning(
                "[BALANCE][FAIL_SOFT][SNAPSHOT_INJECT] state=%s source=%s entry_allowed=0 exit_allowed=1",
                balance_state,
                balance_source,
            )

        if not precomputed_final30_df.empty:
            _assert_engine_boot_locked_final30(run_ctx=run_ctx, final30_df=precomputed_final30_df)
            run_ctx["entry_input_locked_to_final30"] = True
            run_ctx["entry_input_expected_rows"] = 30
            run_ctx["entry_input_expected_codes"] = [
                str(code).zfill(6)
                for code in precomputed_final30_df.get("code", pd.Series(dtype=str)).tolist()
                if str(code).strip()
            ]
            logger.info(
                "[ENTRY_INPUT_LOCK][FINAL30] locked=%s rows=%s universe_ctx_rows=%s source=%s",
                int(bool(run_ctx.get("entry_input_locked_to_final30"))),
                len(precomputed_final30_df),
                len(getattr(universe_ctx, "members", []) or []),
                run_ctx.get("final30_source"),
            )
        engine_runner = PB1Engine(
            universe_repo=universe_repo,
            orders_repo=orders_repo,
            fills_repo=fills_repo,
            positions_repo=positions_repo,
            ledger_repo=ledger_repo,
            kis=kis,
            window=window_name_for_engine,
            window_label=window_name_for_engine,
            phase=phase_name_for_engine,
            dry_run=dry_run_for_engine,  # ✅ bool 강제된 값 전달
            env=env_effective,
            run_id=run_record_id,
            intended_live=intended_live,  # ✅ 메인에서 확정한 LIVE 의도 전달
            strategy=universe_strategy,  # [FIX] watchlist 버그 수정 - strategy 전달
            now_kst_value=now,
            balance_snapshot=balance_snapshot_raw,
            balance_source=balance_source,
            calc_allowed=calc_allowed,  # ✅ 계산 허용 여부
            price_allowed=price_allowed,  # ✅ 가격 조회 허용 여부
            order_allowed=order_allowed,  # ✅ 주문 허용 여부
            entry_block_reason=entry_block_reason,
            minervini_only=minervini_only,  # ✅ MINERVINI_ONLY 모드
            preopen_max_new_positions=PB1_PREOPEN_MAX_NEW_POSITIONS if market_window == "preopen" else 0,
            universe_context=universe_ctx,
            diag_full_exec=diag_full_exec,  # ✅ DIAG 풀패스 플래그 전달
            precomputed_final30_df=precomputed_final30_df,
            precomputed_derived_df=precomputed_derived_df,
            precomputed_universe_df=precomputed_universe_df,
            trade_use_precomputed_features=trade_use_precomputed_features,
            as_of=str(run_ctx.get("derived_as_of") or as_of),
            trade_date=trade_date.isoformat(),
            run_ctx=run_ctx,
            derived_as_of=str(run_ctx.get("derived_as_of") or as_of),
            final30_df=precomputed_final30_df,
            final30_source=str(run_ctx.get("final30_source") or "db_pb1_watchlist_final_scored"),
            final30_locked=bool(run_ctx.get("final30_locked")) and (precomputed_final30_df is not None and not precomputed_final30_df.empty),
            watchlist_final_df=precomputed_final30_df,
            precomputed_features_df=precomputed_derived_df,
            market_window_name=market_window,
            compute_only_full_run=compute_only_full_run,
            force_block_live=bool(compute_only_flags.get("force_block_live")),
            trading_day=trading_day,
            phase_name=phase_name_for_engine,
            window_name=window_name_for_engine,
            force_entry_window_override=force_entry_window_override,
            session_recovery_continue=session_recovery_continue,
            forced_trade_session=forced_trade_session,
            phase_guard_classification=phase_guard_classification,
            # [2026-05-18] KR adaptive entry filter: scanner_context 빌드
            scanner_context={
                "scanner_passed_codes": list(
                    {
                        str(x).zfill(6)
                        for x in (
                            (precomputed_final30_df.get("code", __import__("pandas").Series(dtype=str)).tolist())
                            if (precomputed_final30_df is not None and not precomputed_final30_df.empty)
                            else []
                        )
                        if str(x).strip()
                    }
                ),
            },
        )
        logger.info("[TRADE][ENGINE_BOOT][OK] engine=PB1Engine")
        
        # ✅ DIAG_FULL_EXEC 실행 로그
        if diag_full_exec and mode == "DIAG":
            logger.info(
                "[PB1][DIAG_FULL_EXEC] calling engine.run() window=%s phase=%s dry_run=%s",
                window_label,
                phase_override_arg,
                dry_run,
            )
        logger.info(
            "[ENGINE][ASOF][VERIFY] as_of=%s trade_date=%s source=%s",
            engine_runner.get_as_of(),
            getattr(engine_runner, "_trade_date", None),
            getattr(engine_runner, "_as_of_source", None),
        )
        prep_manifest_path = runtime_root_dir / "prep" / str(run_ctx.get("derived_as_of") or as_of) / "prep_manifest.json"
        if prep_manifest_path.exists():
            try:
                prep_manifest = json.loads(prep_manifest_path.read_text(encoding="utf-8"))
                setattr(engine_runner, "_prep_summary_payload", dict(prep_manifest))
                logger.info(
                    "[PREP][MANIFEST][LOAD] path=%s status=%s final30=%s",
                    prep_manifest_path,
                    prep_manifest.get("build_status"),
                    prep_manifest.get("final30_count"),
                )
            except Exception as exc:
                logger.warning("[PREP][MANIFEST][LOAD_FAIL] path=%s err=%s", prep_manifest_path, exc)
        
        # ✅ ENTRY SCAN: 진입 시그널 스캔 (Phase=entry일 때만)
        entry_signals_result = {}
        entry_scan_compat_failed = False
        if (run_ctx.get("phase_name") or phase_override_arg) == "entry" and not close_cancel_only:
            try:
                locked_watchlist_members = []
                if precomputed_final30_df is not None and not precomputed_final30_df.empty:
                    locked_watchlist_members = [dict(x or {}) for x in precomputed_final30_df.to_dict(orient="records")]
                elif universe_ctx:
                    locked_watchlist_members = list(universe_ctx.members or [])
                watchlist_members = locked_watchlist_members
                
                if watchlist_members:
                    logger.info("[ENTRY_SCAN] start scanning %s symbols", len(watchlist_members))
                    
                    # OHLCV Provider 설정
                    trade_input = (os.getenv("TRADE_INPUT") or "final30").strip().lower() or "final30"
                    trade_precomputed_only = bool(
                        (run_ctx.get("phase_name") or phase_override_arg) == "entry"
                        and trade_use_precomputed_features
                        and not precomputed_final30_df.empty
                        and (run_ctx.get("window_name") or window_label or "").strip().lower() in {"day", "intraday", "after"}
                        and trade_input == "final30"
                    )

                    def ohlcv_provider_for_entry(
                        code: str,
                        days: int,
                        *,
                        usage_context: str | None = None,
                        allow_long_fetch: bool = True,
                        purpose: str | None = None,
                    ):
                        """Entry scan용 OHLCV provider"""
                        try:
                            from trader.data.ohlcv_provider import KISOHLCVProvider
                            provider = KISOHLCVProvider(kis)
                            result = provider.get_ohlcv(
                                code,
                                days,
                                usage_context=usage_context,
                                allow_long_fetch=allow_long_fetch,
                                purpose=purpose,
                            )
                            return result.df
                        except Exception as exc:
                            logger.debug("[ENTRY_SCAN][OHLCV] code=%s days=%s err=%s", code, days, exc)
                            return None
                    
                    # Entry Scan 실행
                    entry_signals_result = scan_all_strategies(
                        watchlist=watchlist_members,
                        ohlcv_provider=ohlcv_provider_for_entry,
                        precomputed_final30_df=precomputed_final30_df,
                        trade_precomputed_only=trade_precomputed_only,
                        data_metrics=(engine_runner._data_metrics if engine_runner else None),
                    )
                    if engine_runner is not None:
                        setattr(engine_runner, "_scanner_summary", dict(entry_signals_result.get("summary") or {}))
                        setattr(engine_runner, "_scanner_evaluations", list(entry_signals_result.get("evaluations") or []))
                    
                    logger.info(
                        "[ENTRY_SCAN] completed - breakout=%s pullback=%s momentum=%s unique=%s",
                        len(entry_signals_result.get("breakout", [])),
                        len(entry_signals_result.get("pullback", [])),
                        len(entry_signals_result.get("momentum", [])),
                        len(entry_signals_result.get("all", [])),
                    )
                    
                    # 시그널 요약 로그
                    for signal in entry_signals_result.get("all", [])[:5]:  # 상위 5개만 로그
                        logger.info(
                            "[ENTRY_SIGNAL] %s %s: strategy=%s strength=%.3f close=%.0f",
                            signal.code,
                            signal.name,
                            signal.strategy,
                            signal.signal_strength,
                            signal.close,
                        )

                    # [2026-05-18] KR rescue: entry scan 결과로 scanner_context 업데이트
                    # engine 생성 시점에는 precomputed_final30_df 기반 codes만 있었으나
                    # 실제 entry scan 후에는 전략별 passed_codes / 집계를 반영한다.
                    if engine_runner is not None:
                        _all_scan_codes = list({
                            str(s.code)
                            for s in entry_signals_result.get("all", [])
                            if getattr(s, "code", None)
                        })
                        _existing_ctx = getattr(engine_runner, "scanner_context", {}) or {}
                        engine_runner.scanner_context = {
                            **_existing_ctx,
                            "scanner_passed_codes": _all_scan_codes or _existing_ctx.get("scanner_passed_codes", []),
                            "raw_signal_setup_ok": len(entry_signals_result.get("all", [])),
                            "scanner_passed": len(entry_signals_result.get("all", [])),
                            "pullback_pass": len(entry_signals_result.get("pullback", [])),
                            "breakout_pass": len(entry_signals_result.get("breakout", [])),
                            "momentum_pass": len(entry_signals_result.get("momentum", [])),
                        }
                        logger.info(
                            "[ENTRY_SCAN][KR_CONTEXT_UPDATED] scanner_passed=%s pullback=%s breakout=%s momentum=%s",
                            len(_all_scan_codes),
                            len(entry_signals_result.get("pullback", [])),
                            len(entry_signals_result.get("breakout", [])),
                            len(entry_signals_result.get("momentum", [])),
                        )
                else:
                    logger.error(
                        "[ENTRY_SCAN][LOCK_MISSING] final30_locked=%s derived_as_of=%s",
                        int(bool(run_ctx.get("final30_locked"))),
                        str(run_ctx.get("derived_as_of") or as_of),
                    )
            except Exception as exc:
                entry_scan_compat_failed = True
                logger.warning(
                    "[ENTRY_SCAN][COMPAT_FAIL] reason=%s fallback=pb1_engine_internal_scan severity=non_fatal",
                    exc,
                    exc_info=True,
                )
        
        if close_cancel_only:
            result = engine_runner.run_close_cancel()
        else:
            engine_started = True
            try:
                result = engine_runner.run()
                engine_completed = True
                orders_accepted_count = int((getattr(engine_runner, "_run_summary_payload", {}) or {}).get("submitted", 0))
            except Exception as exc:
                exit_summary = getattr(engine_runner, "_exit_summary_payload", {}) or {}
                sell_ack = int(exit_summary.get("sell_orders_ack") or exit_summary.get("accepted_sells") or exit_summary.get("accepted_sell_count") or 0)
                sell_filled = int(exit_summary.get("sell_orders_filled") or exit_summary.get("fill_confirmed_sells") or exit_summary.get("fill_confirmed_sell_count") or 0)
                if sell_ack > 0:
                    logger.error(
                        "[PB1][PARTIAL_OK][EXIT_DONE_ENTRY_FAILED] sell_orders_ack=%s sell_orders_filled=%s err_type=%s err=%s",
                        sell_ack,
                        sell_filled,
                        type(exc).__name__,
                        exc,
                    )
                    return touched_files, True, {
                        "buy_orders": 0,
                        "sell_orders": sell_ack,
                        "sell_orders_ack": sell_ack,
                        "sell_orders_filled": sell_filled,
                        "warning_counts": {"entry_failed_after_exit": 1},
                    }, phase_for_log, "PARTIAL_OK_EXIT_DONE_ENTRY_FAILED"
                raise
            if entry_scan_compat_failed:
                logger.info("[ENTRY_SCAN][FALLBACK] source=pb1_engine_internal_scan status=ok")

        postprocess_stage = "result_postprocess"
        logger.info(
            "[PB1][RUN_ONCE][FAIL_OPEN_FLAG] runs_ledger_fail_open=%s loop_mode=%s",
            int(bool(runs_ledger_fail_open)),
            int(bool(loop_mode)),
        )

        if result is not None:
            result_reason = result.notes or "none"
            summary_session = str(os.getenv("PB1_SESSION_KIND") or window_label or phase_for_log or "unknown")
            summary_event = str(os.getenv("GITHUB_EVENT_NAME") or "unknown")
            pm_policy_reason = str(os.getenv("PB1_PM_POLICY_REASON") or "").strip()
            if not trading_day and (compute_only_full_run or dry_run or bool(compute_only_flags.get("force_block_live"))):
                result.status = "OK"
                result_reason = "WINDOW_BLOCKED_COMPUTE_ONLY"
            elif market_window == "after" and compute_only_full_run:
                result.status = "OK"
                result_reason = "WINDOW_BLOCKED_COMPUTE_ONLY"
            elif result.status in {"SKIP", "SKIPPED"}:
                result.status = "SKIP_PHASE_WINDOW"
                result_reason = result.notes or "PHASE_WINDOW_BLOCKED"
            elif summary_session == "pm" and pm_policy_reason and result.status in {"OK_NO_TRADE", "NO_TRADE", "OK_NO_CANDIDATES"}:
                result.status = "OK_NO_TRADE"
                result_reason = pm_policy_reason
            elif result.status in {"OK_NO_TRADE", "NO_TRADE"} and result.notes and "no_candidates" in result.notes:
                result.status = "OK_NO_TRADE"
                result_reason = "NO_ORDERABLE_CANDIDATES"
            elif result.status in {"OK_NO_TRADE", "NO_TRADE"}:
                result.status = "OK_NO_TRADE"
                result_reason = result.notes or "NO_ORDER_INTENTS"
            elif result.status == "WARN_FAIL_OPEN":
                result.status = "OK_WITH_WARNINGS"
                result_reason = result.notes or "FAIL_OPEN_WARNING"
            elif result.status == "DEGRADED_POSTPROCESS":
                result.status = "OK_DEGRADED"
                result_reason = result.notes or "DEGRADED_POSTPROCESS"
            if runs_ledger_fail_open and result.status not in {"FATAL_RUNTIME", "SKIP_PHASE_WINDOW"}:
                result.status = "OK_WITH_WARNINGS"
                result_reason = "runs_ledger_fail_open"
            if _is_close_manual_replay_active() and result.status not in {"FATAL_RUNTIME", "FATAL_POSTPROCESS", "SKIP_PHASE_WINDOW"}:
                replay_reason = f"CLOSE_MANUAL_REPLAY_{_resolve_close_manual_mode().upper()}"
                result.status = "OK_MANUAL_REPLAY"
                result_reason = replay_reason if result_reason in {"", "none"} else f"{replay_reason}:{result_reason}"
            logger.info(
                "[RUN_SUMMARY][RESULT] status=%s reason=%s session=%s event=%s",
                result.status,
                result_reason,
                summary_session,
                summary_event,
            )
            result_warning_counts = _normalize_warning_counts(
                getattr(result, "warning_counts", None) or getattr(engine_runner, "_warning_counts", None)
            )
            result_terminal_state = getattr(result, "terminal_state", None) or _resolve_session_terminal_state(
                result_status=result.status,
                exit_reason=result_reason,
                warning_counts=result_warning_counts,
            )
            logger.info(
                "[PB1][SESSION_TERMINAL] terminal_state=%s result_status=%s exit_reason=%s warnings_total=%s",
                result_terminal_state,
                result.status,
                result_reason,
                _warning_total(result_warning_counts),
            )
            logger.info(
                "[PB1][SESSION_WARNINGS] timeout_count=%s db_read_fail_open_count=%s ledger_fail_first_count=%s degraded_stage_count=%s duplicate_skip_count=%s db_engine_dispose_count=%s",
                result_warning_counts.get("timeout_count", 0),
                result_warning_counts.get("db_read_fail_open_count", 0),
                result_warning_counts.get("ledger_fail_first_count", 0),
                result_warning_counts.get("degraded_stage_count", 0),
                result_warning_counts.get("duplicate_skip_count", 0),
                result_warning_counts.get("db_engine_dispose_count", 0),
            )
            result.notes = result_reason
            if nontrading_eval_mode:
                _write_nontrading_eval_reports(
                    engine_runner=engine_runner,
                    result_status=result.status,
                )
        try:
            scanner_summary = getattr(engine_runner, "_scanner_summary", {}) or {}
            run_summary = getattr(engine_runner, "_run_summary_payload", {}) or {}
            tick_label = os.getenv("PB1_LOOP_TICK_INDEX") or str(run_record_id or "1")
            price_stats = get_price_runtime_stats(reset=True)
            breaker_stats = get_breaker_runtime_stats(reset=True)
            scanner_usable = int(scanner_summary.get("usable_count", scanner_summary.get("total", 0)))
            raw_signal_setup_ok = int(scanner_summary.get("setup_ok_count", 0))
            pb1_filter_setup_ok = int(run_summary.get("setup_ok", 0))
            if scanner_summary:
                logger.info(
                    "[ENTRY_SCAN][RAW_SIGNAL_SUMMARY] usable=%s raw_signal_setup_ok=%s total=%s",
                    scanner_usable,
                    raw_signal_setup_ok,
                    scanner_summary.get("total", 0),
                )
                logger.info(
                    "[ENGINE][ORDERABILITY_SUMMARY] scanned=%s pb1_filter_setup_ok=%s risk_ok=%s sized_ok=%s buyable_ok=%s order_candidates=%s submitted=%s",
                    run_summary.get("scanned", 0),
                    pb1_filter_setup_ok,
                    run_summary.get("risk_ok", 0),
                    run_summary.get("sized_ok", 0),
                    run_summary.get("buyable_ok", 0),
                    run_summary.get("order_candidates", 0),
                    run_summary.get("submitted", 0),
                )
            if scanner_summary:
                mismatch_categories = {
                    "pb1_filter_drop": max(0, raw_signal_setup_ok - pb1_filter_setup_ok),
                    "risk_gate_drop": max(0, int(run_summary.get("setup_ok", 0)) - int(run_summary.get("risk_ok", 0))),
                    "sizing_drop": max(0, int(run_summary.get("risk_ok", 0)) - int(run_summary.get("sized_ok", 0))),
                    "buyable_drop": max(0, int(run_summary.get("sized_ok", 0)) - int(run_summary.get("buyable_ok", 0))),
                }
                logger.info(
                    "[CONSISTENCY][SCAN_ENGINE] scanner_usable=%s raw_signal_setup_ok=%s pb1_filter_setup_ok=%s risk_ok=%s sized_ok=%s buyable_ok=%s categories=%s",
                    scanner_usable,
                    raw_signal_setup_ok,
                    pb1_filter_setup_ok,
                    int(run_summary.get("risk_ok", 0)),
                    int(run_summary.get("sized_ok", 0)),
                    int(run_summary.get("buyable_ok", 0)),
                    mismatch_categories,
                )
            state_label = "ORDER_SUBMITTED" if int(run_summary.get("submitted", 0)) > 0 else "NO_ORDERABLE"
            # 세션별 HEARTBEAT prefix 동적 결정 - close는 반드시 TRADE_CLOSE
            _hb_session_kind = os.getenv("PB1_SESSION_KIND") or os.getenv("PB1_FORCE_TRADE_SESSION") or "unknown"
            _hb_window = os.getenv("FORCE_MARKET_WINDOW") or ""
            _hb_phase = (os.getenv("FORCE_PB1_PHASE") or os.getenv("PB1_PHASE_DEFAULT") or "").strip().lower()
            
            if _hb_session_kind == "close" or _hb_window == "close":
                _hb_prefix = "TRADE_CLOSE"
            elif _hb_session_kind == "am":
                _hb_prefix = "TRADE_AM"
            elif _hb_session_kind in ("afternoon", "pm"):
                # PM entry phase uses TRADE_AFTERNOON, exit phase uses TRADE_CLOSE
                _hb_prefix = "TRADE_CLOSE" if _hb_phase == "exit" else "TRADE_AFTERNOON"
            else:
                _hb_prefix = f"TRADE_{_hb_session_kind.upper()}"
            logger.info(
                "[%s][HEARTBEAT] tick=%s state=%s candidates_scanned=%s setup_ok=%s risk_ok=%s sized_ok=%s buyable_ok=%s submitted=%s top_blockers=%s",
                _hb_prefix,
                tick_label,
                state_label,
                int(run_summary.get("scanned", 0)),
                int(run_summary.get("setup_ok", 0)),
                int(run_summary.get("risk_ok", 0)),
                int(run_summary.get("sized_ok", 0)),
                int(run_summary.get("buyable_ok", 0)),
                int(run_summary.get("submitted", 0)),
                run_summary.get("blocked_by", "none") or "none",
            )
            logger.info(
                "[%s][HEARTBEAT] tick=%s late_start=%s degraded_session=%s rate_limit_count=%s breaker_open_count=%s breaker_block_count=%s",
                _hb_prefix,
                tick_label,
                int(str(os.getenv("TRADE_AM_LATE_START") or "0") == "1"),
                int(str(os.getenv("TRADE_AM_DEGRADED_SESSION") or "0") == "1"),
                int(price_stats.get("price_http_fail_count", 0)),
                int(breaker_stats.get("open_count", 0)),
                int(breaker_stats.get("blocked_count", 0)),
            )
            logger.info(
                "[PB1][RATE_LIMIT][SUMMARY] tick=%s price_http_fail_count=%s retry_count=%s cache_hit=%s cache_miss=%s breaker_open_count=%s breaker_block_count=%s",
                tick_label,
                int(price_stats.get("price_http_fail_count", 0)),
                int(price_stats.get("retry_count", 0)),
                int(price_stats.get("cache_hit", 0)),
                int(price_stats.get("cache_miss", 0)),
                int(breaker_stats.get("open_count", 0)),
                int(breaker_stats.get("blocked_count", 0)),
            )
        except Exception as exc:
            logger.warning("[CONSISTENCY][SCAN_ENGINE][FAIL] err=%s", exc)
        
        # ✅ RUN 요약 JSON 생성
        try:
            trace_id = getattr(engine_runner, 'run_id', str(run_record_id))
            as_of_used = getattr(engine_runner, '_universe_as_of', None)
            generate_run_summary_json(
                run_id=str(run_record_id),
                trace_id=str(trace_id),
                env=env_effective,
                engine=engine_runner,
                as_of_requested=None,
                as_of_used=as_of_used,
                watchlist_as_of=None,
                universe_as_of=None,
                fallback_used=False,
            )
            try:
                from trader.diagnostics.run_validator import validate_run_pipeline

                run_summary_payload = getattr(engine_runner, "_run_summary_payload", {}) or {}
                top_blockers_raw = run_summary_payload.get("blocked_by", "") or ""
                top_blockers: dict[str, int] = {}
                if isinstance(top_blockers_raw, str):
                    for part in top_blockers_raw.split(","):
                        if ":" not in part:
                            continue
                        key, value = part.split(":", 1)
                        try:
                            top_blockers[key.strip().upper()] = int(float(value.strip()))
                        except Exception:
                            continue
                validate_run_pipeline(
                    session_kind=str(os.getenv("PB1_SESSION_KIND") or summary_session),
                    phase=str(run_ctx.get("phase_name") or phase_for_log),
                    final30_rows=int(run_ctx.get("final30_rows") or (len(precomputed_final30_df) if isinstance(precomputed_final30_df, pd.DataFrame) else 0) or 0),
                    entry_enabled=bool(run_ctx.get("entry_enabled", os.getenv("PB1_ENTRY_ENABLED") == "1")),
                    exit_only=bool(run_ctx.get("exit_only", os.getenv("PB1_EXIT_ONLY_MODE") == "1")),
                    mode_applied=bool(run_ctx.get("entry_enabled") is True and str(run_ctx.get("phase_name") or "") == "pm_entry"),
                    entry_decision_seen=bool(run_summary_payload),
                    setup_ok_seen="setup_ok" in run_summary_payload,
                    order_candidates_seen="order_candidates" in run_summary_payload,
                    buyable_gate_seen="buyable_ok" in run_summary_payload,
                    order_submit_seen=int(run_summary_payload.get("submitted", 0) or 0) > 0,
                    no_buy_reason=str(run_summary_payload.get("no_trade_reason") or result_reason or ""),
                    top_blockers=top_blockers,
                    order_candidates_count=int(run_summary_payload.get("order_candidates", 0) or 0),
                    now=now,
                    artifact_dir="artifacts",
                )
            except Exception as validator_exc:
                logger.warning("[RUN_VALIDATOR][FAIL] err=%s", validator_exc)
        except Exception as e:
            logger.warning("[RUN_SUMMARY][GENERATE_FAIL] %s", type(e).__name__, exc_info=False)
        
        # Save top_candidates for next run to limit OHLCV queries
        if engine_runner and hasattr(engine_runner, 'top_candidates'):
            top_candidates_path = runtime_path("top_candidates.json")
            try:
                with open(top_candidates_path, 'w') as f:
                    json.dump(engine_runner.top_candidates, f)
                logger.info("[PB1][TOP_CANDIDATES][SAVE] saved %s to %s", len(engine_runner.top_candidates), top_candidates_path)
            except Exception as exc:
                logger.warning("[PB1][TOP_CANDIDATES][SAVE_FAIL] %s", exc)
        did_work = True
        runs_repo.finish_run(run_record_id, status=result.status, notes=result.notes)
        db_write_reasons.append("run_finish")
        _write_last_db_write(
            runtime_root_dir,
            run_id=str(run_record_id),
            reason=",".join(db_write_reasons),
            now=now,
        )
        touched_files = engine_runner.get_touched_files() if engine_runner and hasattr(engine_runner, "get_touched_files") else []
    except Exception as exc:
        # KRX TickTimeoutError는 traceback 없이 기록 (traceback_seen 방지)
        _guard_is_tick_timeout = (
            exc.__class__.__name__ == "TickTimeoutError"
            or "tick_hard_timeout" in str(exc)
            or ("timeout_sec=" in str(exc) and "last_stage=" in str(exc))
        )
        _guard_is_krx = (
            str(os.getenv("PB1_MARKET_SCOPE") or "").upper() in {"KRX"}
            or str(os.getenv("MARKET") or "").upper() == "KR"
            or str(os.getenv("EXCHANGE") or "").upper() == "KRX"
        )
        if _guard_is_tick_timeout and _guard_is_krx:
            logger.error(
                "[PB1][FATAL_GUARD] tick_timeout_krx strategy_env=%s kis_env=%s postprocess_stage=%s err_type=%s err=%s",
                locals().get("env_strategy"),
                locals().get("env_kis"),
                postprocess_stage,
                type(exc).__name__,
                exc,
            )
        else:
            logger.exception(
                "[PB1][FATAL_GUARD] unexpected error strategy_env=%s kis_env=%s derived_env=%s effective_env=%s postprocess_stage=%s",
                locals().get("env_strategy"),
                locals().get("env_kis"),
                locals().get("env_derived"),
                locals().get("env_effective"),
                postprocess_stage,
            )
        phase_context = phase_for_log or phase_override_arg or "unknown"
        window_context = window_label or "unknown"
        current_code = engine_runner.current_code if engine_runner else None
        top_candidates = engine_runner.top_candidates if engine_runner else []
        logger.error(
            "[PB1][FAIL][CONTEXT] phase=%s window=%s current_code=%s top_candidates=%s",
            phase_context,
            window_context,
            current_code,
            top_candidates,
        )
        fatal_status = "FATAL_RUNTIME"
        fatal_reason = "RUNS_LEDGER_QUERY_FAIL" if str(exc) == "RUNS_LEDGER_QUERY_FAIL" else "UNHANDLED_RUNTIME_EXCEPTION"

        # KRX 한국장: positions_lookup 또는 account_reconcile 단계에서 TickTimeoutError를
        # FATAL_RUNTIME이 아닌 RECOVERABLE_DB_TIMEOUT으로 분류한다.
        _last_stage_at_fatal = str(os.getenv("PB1_LAST_STAGE") or "")
        _is_tick_timeout = (
            exc.__class__.__name__ == "TickTimeoutError"
            or "tick_hard_timeout" in str(exc)
            or ("timeout_sec=" in str(exc) and "last_stage=" in str(exc))
        )
        _is_krx_context_fatal = (
            str(os.getenv("PB1_MARKET_SCOPE") or "").upper() in {"KRX"}
            or str(os.getenv("MARKET") or "").upper() == "KR"
            or str(os.getenv("EXCHANGE") or "").upper() == "KRX"
        )
        _is_positions_or_reconcile_stage = any(
            token in _last_stage_at_fatal
            for token in (
                "positions_lookup",
                "positions.list_positions",
                "account_reconcile",
            )
        )
        if _is_tick_timeout and _is_krx_context_fatal and _is_positions_or_reconcile_stage:
            fatal_status = "RECOVERABLE_DB_TIMEOUT"
            fatal_reason = "POSITIONS_LOOKUP_TIMEOUT"
            logger.warning(
                "[PB1][FATAL_GUARD][RECOVERABLE] kind=tick_timeout last_stage=%s krx=1 "
                "reclassified=RECOVERABLE_DB_TIMEOUT",
                _last_stage_at_fatal,
            )
            dispose_engine_safely(engine, reason=f"FATAL_GUARD:POSITIONS_LOOKUP_TIMEOUT:{_last_stage_at_fatal}")
        elif _is_tick_timeout and _is_krx_context_fatal:
            fatal_status = "RECOVERABLE_DB_TIMEOUT"
            fatal_reason = "TICK_TIMEOUT_KRX"
            logger.warning(
                "[PB1][FATAL_GUARD][RECOVERABLE] kind=tick_timeout last_stage=%s krx=1 "
                "reclassified=RECOVERABLE_DB_TIMEOUT",
                _last_stage_at_fatal,
            )
            dispose_engine_safely(engine, reason=f"FATAL_GUARD:TICK_TIMEOUT_KRX:{_last_stage_at_fatal}")
        _distinguish_postprocess = str(os.getenv("PB1_ASSERT_DISTINGUISH_POSTPROCESS_FAILURE", "1")).strip() in {"1", "true", "yes"}
        if postprocess_stage != "engine_run":
            if engine_completed and orders_accepted_count > 0 and _distinguish_postprocess:
                fatal_status = "PARTIAL_SUCCESS_POSTPROCESS_FAILED"
                fatal_reason = f"ENGINE_RAN_ORDER_ACCEPTED_POSTPROCESS_FAILED:{type(exc).__name__.upper()}"
            else:
                fatal_status = "FATAL_POSTPROCESS"
                fatal_reason = f"RUN_ONCE_POSTPROCESS_{type(exc).__name__.upper()}"
        logger.error(
            "[ASSERT][ENGINE] engine_started=%s engine_completed=%s postprocess_stage=%s",
            int(engine_started),
            int(engine_completed),
            postprocess_stage,
        )
        logger.error(
            "[ASSERT][ORDER] orders_accepted=%s buy_orders_in_payload=%s",
            orders_accepted_count,
            int((getattr(engine_runner, "_run_summary_payload", {}) or {}).get("submitted", 0)) if engine_runner else 0,
        )
        logger.error(
            "[ASSERT][POSTPROCESS] fatal_status=%s fatal_reason=%s distinguish_enabled=%s",
            fatal_status,
            fatal_reason,
            int(_distinguish_postprocess),
        )
        logger.error(
            "[ASSERT][RESULT] status=%s reason=%s session=%s event=%s",
            fatal_status,
            fatal_reason,
            str(os.getenv("PB1_SESSION_KIND") or window_context or phase_context or "unknown"),
            str(os.getenv("GITHUB_EVENT_NAME") or "unknown"),
        )
        logger.error(
            "[RUN_SUMMARY][RESULT] status=%s reason=%s session=%s event=%s",
            fatal_status,
            fatal_reason,
            str(os.getenv("PB1_SESSION_KIND") or window_context or phase_context or "unknown"),
            str(os.getenv("GITHUB_EVENT_NAME") or "unknown"),
        )
        logger.error("[PB1][EXIT] reason=fatal_runtime")
        if run_record_id:
            runs_repo.finish_run(run_record_id, status=fatal_status, notes=fatal_reason)
            _write_last_db_write(runtime_root_dir, run_id=str(run_record_id), reason="failed", now=now)
        raise
    finally:
        pass
    metrics = {
        "balance_api_calls": result.balance_api_calls if result else 0,
        "balance_cache_hits": result.balance_cache_hits if result else 0,
        "balance_tick_cache_hits": result.balance_tick_cache_hits if result else 0,
        "buy_orders": int((getattr(engine_runner, "_run_summary_payload", {}) or {}).get("submitted", 0)),
        "sell_orders": int((getattr(engine_runner, "_exit_summary_payload", {}) or {}).get("submitted", 0)),
        "sell_orders_ack": int((getattr(engine_runner, "_exit_summary_payload", {}) or {}).get("accepted_sell_count", 0)),
        "exit_evaluated_positions": int((getattr(engine_runner, "_exit_summary_payload", {}) or {}).get("evaluated_count", 0)),
        "exit_submitted_codes": sorted(getattr(engine_runner, "session_exit_submitted_codes", set()) or []),
        "no_sellable_qty_terminal_codes": sorted(getattr(engine_runner, "no_sellable_qty_terminal_codes", set()) or []),
        "order_candidates": int((getattr(engine_runner, "_run_summary_payload", {}) or {}).get("order_candidates", 0)),
        "api_submitted": int((getattr(engine_runner, "_run_summary_payload", {}) or {}).get("api_submitted", (getattr(engine_runner, "_run_summary_payload", {}) or {}).get("submitted", 0))),
        "degraded": bool(getattr(result, "status", "") == "DEGRADED_POSTPROCESS"),
        "warning_counts": _normalize_warning_counts(getattr(result, "warning_counts", None)),
        "terminal_state": getattr(result, "terminal_state", None),
    }
    result_status = result.status if result else "UNKNOWN"
    
    # ✅ DIAG 모드 실행 요약 로그
    if mode == "DIAG":
        logger.info(
            "[DIAG_SUMMARY] status=%s phase=%s did_work=%s metrics=%s",
            result_status,
            phase_for_log,
            did_work,
            metrics,
        )
        if engine_runner:
            logger.info(
                "[DIAG_SUMMARY][ENGINE] top_candidates=%s current_code=%s",
                len(engine_runner.top_candidates) if hasattr(engine_runner, 'top_candidates') else 0,
                engine_runner.current_code if hasattr(engine_runner, 'current_code') else None,
            )
    
    return touched_files, did_work, metrics, phase_for_log, result_status


def _run_nontrading_smoke_if_needed(
    *,
    runtime_root_dir: Path,
    engine,
    now: datetime,
    strategy_mode: str,
) -> bool:
    if strategy_mode != "DIAG":
        return False
    _, trading_day, _market_window, _mode_source = resolve_strategy_mode(
        now_kst=now,
        force_mode_env=os.getenv("FORCE_STRATEGY_MODE"),
    )
    if trading_day:
        return False
    
    # ✅ CRITICAL: nontrading smoke도 derived_as_of 사용
    smoke_derived_as_of_date = resolve_derived_as_of(now)
    as_of = smoke_derived_as_of_date.isoformat()
    
    logger.info(
        "[ASOF][NONTRADING_SMOKE] trade_date=%s derived_as_of=%s",
        now.date().isoformat(),
        as_of,
    )
    
    smoke_flag = nontrading_smoke_flag_path(runtime_root_dir, as_of=as_of)
    if smoke_flag.exists() and not NONTRADING_SMOKE_FORCE:
        logger.info("[NONTRADING_SMOKE][SKIP] reason=already_done flag=%s", smoke_flag)
        return False
    run_nontrading_smoke_once(
        runtime_root_dir=runtime_root_dir,
        engine=engine,
        now=now,
        env=(os.getenv("KIS_ENV") or "practice").lower(),
        strategy=os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY,
        as_of=as_of,
        timeout_sec=NONTRADING_SMOKE_TIMEOUT_SEC,
        db_store=NONTRADING_SMOKE_DB_STORE,
        force_rebuild=NONTRADING_SMOKE_FORCE_REBUILD,
    )
    write_nontrading_smoke_flag(
        runtime_root_dir,
        now=now,
        run_id=os.getenv("TRADER_RUN_ID", "local"),
        sha=os.getenv("GITHUB_SHA", "unknown"),
        as_of=as_of,
    )
    return True


def _run_loop(*, args: argparse.Namespace) -> None:
    loop_interval = _parse_int_env("PB1_LOOP_INTERVAL_SEC", 60)
    _run_loop_minutes, _loop_max_minutes, max_seconds, _ = _resolve_loop_limits()
    now = _get_now_kst()
    session_kind = _resolve_session_kind()
    session_end_dt = _resolve_session_end_dt(now)
    loop_deadline = session_end_dt

    logger.info(
        "[PB1][LOOP][SESSION] kind=%s now=%s session_end=%s interval=%s",
        session_kind,
        now.isoformat(),
        session_end_dt.isoformat(),
        loop_interval,
    )
    
    logger.info(
        "[PB1][LOOP] start now_kst=%s max_seconds=%s",
        now.isoformat(),
        max_seconds,
    )

    total_start_ts = time_mod.monotonic()
    engine = make_engine()
    # Dedicated lock-scope connection only; trading persistence uses repository
    # connections from ``engine`` and is never part of this rollback scope.
    lock_conn = engine.connect()
    lock_context = (
        f"mode=loop "
        f"strategy_env={os.getenv('STRATEGY_ENV') or os.getenv('KIS_ENV') or os.getenv('ENV')} "
        f"session={os.getenv('PB1_SESSION_KIND') or os.getenv('SESSION_KIND') or os.getenv('FORCE_MARKET_WINDOW') or 'unknown'} "
        f"workflow={os.getenv('GITHUB_WORKFLOW')} "
        f"job={os.getenv('GITHUB_JOB')} "
        f"run_id={os.getenv('GITHUB_RUN_ID')} "
        f"run_attempt={os.getenv('GITHUB_RUN_ATTEMPT')} "
        f"trader_run_id={os.getenv('TRADER_RUN_ID')}"
    )
    if not try_acquire_lock(lock_conn, context=lock_context):
        logger.warning(
            "[PB1][LOOP][SKIP_LOCKED] run lock unavailable owner_logged=1 action=safe_skip context=%s",
            lock_context,
        )
        lock_conn.close()
        return
    exit_reason = "unknown"
    sticky_precheck_reason: str | None = None
    sticky_precheck_count = 0
    sticky_runtime_signature: tuple[str, str] | None = None
    sticky_runtime_count = 0
    balance_api_calls = 0
    balance_cache_hits = 0
    balance_tick_cache_hits = 0
    session_warning_counts = {key: 0 for key in SESSION_WARNING_KEYS}
    last_phase = "none"
    last_result_status = "UNKNOWN"
    loop_started_ts = total_start_ts
    runtime_root_dir = runtime_root()
    
    # ✅ run_id SSOT: TRADER_RUN_ID를 사용하여 통일
    from uuid import uuid4
    run_id = os.getenv("TRADER_RUN_ID")
    if not run_id:
        run_id = str(uuid4())
        os.environ["TRADER_RUN_ID"] = run_id
    
    # Create RunContext for loop mode
    env = resolve_env(getattr(args, "env", None))
    strategy = os.getenv("STRATEGY", "best_k_meta")
    workflow_run_id = str(os.getenv("GITHUB_RUN_ID") or "").strip() or None
    gh_run_number = _parse_optional_int_env("GITHUB_RUN_ID") or _parse_optional_int_env("GITHUB_RUN_NUMBER")
    workflow_attempt = _parse_optional_int_env("GITHUB_RUN_ATTEMPT")
    event_name = str(os.getenv("GITHUB_EVENT_NAME") or "").strip().lower() or "schedule"
    git_sha = os.getenv("GITHUB_SHA")
    exec_mode = resolve_mode(os.getenv("STRATEGY_MODE", "DIAG"))
    ctx = RunContext.new(
        account_env=env,
        exec_mode=exec_mode,
        strategy=strategy,
        gh_run_number=gh_run_number,
        git_sha=git_sha,
    )
    logger.info(
        "[PB1][LOOP][CONTEXT] run_id=%s env=%s strategy=%s gh_run_number=%s git_sha=%s",
        run_id,
        ctx.env,
        ctx.strategy,
        ctx.gh_run_number,
        ctx.git_sha,
    )
    runs_repo = RunsRepo(engine)
    session_guard_run_id: str | None = None
    runs_ledger_fail_open = False
    guard_result = _acquire_session_guard_or_takeover(
        runs_repo=runs_repo,
        env=ctx.env,
        session_kind=session_kind,
        run_id=run_id,
        workflow_run_id=workflow_run_id,
        workflow_attempt=workflow_attempt,
        event_name=event_name,
        workflow=os.getenv("GITHUB_WORKFLOW"),
        git_sha=ctx.git_sha,
        strategy_env=os.getenv("STRATEGY_ENV"),
        kis_env=os.getenv("KIS_ENV"),
        ctx_env=ctx.env,
    )
    runs_ledger_fail_open = bool(guard_result.get("fail_open"))
    session_guard_run_id = guard_result.get("session_guard_run_id")
    if guard_result.get("blocked"):
        return
    _touch_session_guard(
        runs_repo=runs_repo,
        session_guard_run_id=session_guard_run_id,
        session_kind=session_kind,
        status="SESSION_GUARD_STARTED",
    )
    
    try:
        _ensure_bootstrap_migrations(engine)
        _write_change_flag(False, ["init"])
        trade_start_ts = time_mod.monotonic()
        loop_started_ts = trade_start_ts
        now_kst_value = _get_now_kst()
        logger.info(
            "[PB1][CLOCK] total_start=%.3f trade_start=%.3f",
            total_start_ts,
            trade_start_ts,
        )
        logger.info(
            "[PB1][LOOP] deadline_ready now_kst=%s deadline=%s max_seconds=%s baseline=trade",
            now_kst_value.isoformat(),
            session_end_dt.isoformat(),
            max_seconds,
        )
        # ✅ PB1_CANDIDATE_ONLY=1이면 universe ensure skip (후보군만 사용)
        # ✅ 또는 PB1_JOB=TRADE_INTRADAY이고 watchlist 사용 중이면 skip
        skip_universe = False
        skip_reason = ""
        
        if os.getenv("PB1_CANDIDATE_ONLY", "0") == "1":
            skip_universe = True
            skip_reason = "PB1_CANDIDATE_ONLY=1"
        elif os.getenv("PB1_JOB", "TRADE_INTRADAY").upper() == "TRADE_INTRADAY":
            # trade 모드에서 watchlist 사용 시 universe 로드 차단
            from trader.db.repos import WatchlistRepo
            try:
                watchlist_repo = WatchlistRepo(engine)
                watchlist_count = watchlist_repo.count_as_of(
                    env=(os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice"),
                    strategy=os.getenv("WATCHLIST_FINAL_SCORED_STRATEGY_KEY", os.getenv("WATCHLIST_FINAL_STRATEGY_KEY", "pb1_watchlist_final_scored")),
                    as_of=now_kst_value.date().isoformat()
                )
                if watchlist_count > 0:
                    skip_universe = True
                    skip_reason = f"watchlist_only mode=trade watchlist={watchlist_count}"
            except Exception as e:
                logger.warning("[UNIVERSE][CHECK][FAIL] watchlist check failed: %s", e)
        
        if skip_universe:
            logger.info("[UNIVERSE][SKIP] reason=%s -> skip ensure/build, will use candidate pool/watchlist only", skip_reason)
        else:
            # ✅ CRITICAL: ensure_universe_built_once도 derived_as_of 사용
            loop_derived_as_of_date = resolve_derived_as_of(now_kst_value)
            loop_as_of = loop_derived_as_of_date.isoformat()
            
            logger.info(
                "[ASOF][LOOP][UNIVERSE] trade_date=%s derived_as_of=%s",
                now_kst_value.date().isoformat(),
                loop_as_of,
            )
            
            ensure_universe_built_once(
                engine=engine,
                as_of=loop_as_of,
            )
        strategy_mode = (
            getattr(args, "strategy_mode", None)
            or os.getenv("EFFECTIVE_STRATEGY_MODE")
            or os.getenv("STRATEGY_MODE")
            or ""
        )
        strategy_mode = str(strategy_mode).upper()
        kis_factory = lambda: KisAPI()
        if strategy_mode == "DIAG":
            logger.info("[DIAG][BALANCE] probe_once start")
            _diag_balance_probe_once_safe(
                logger=logger,
                runtime_root_dir=runtime_root_dir,
                kis_factory=kis_factory,
            )
        stop_requested = {"value": False}

        def _request_stop(reason: str) -> None:
            if stop_requested["value"]:
                return
            stop_requested["value"] = True
            logger.warning("[PB1][SIGNAL] %s -> stopping loop", reason)

        signal.signal(signal.SIGTERM, lambda *_args: _request_stop("SIGTERM"))
        signal.signal(signal.SIGINT, lambda *_args: _request_stop("SIGINT"))
        tick_index = 0
        ticks_total = 0
        ticks_ok = 0
        ticks_no_trade = 0
        ticks_fatal = 0
        ticks_degraded = 0
        buy_orders = 0
        sell_orders = 0
        session_metrics: dict[str, int] = {
            "ticks_total": 0,
            "buy_orders": 0,
            "buy_orders_ack": 0,
            "sell_orders": 0,
            "sell_orders_ack": 0,
            "order_candidates": 0,
            "api_submitted": 0,
            "accepted": 0,
            "filled_confirmed": 0,
            "partial_filled_confirmed": 0,
            "rejected": 0,
            "skipped": 0,
        }
        # A loop owns no PB1Engine instance: every tick creates and finalizes one
        # inside run_once. Preserve only its returned summary for session markers.
        # Do not reference an unqualified engine_runner here (it caused the AM/PM
        # finalizer NameError before pb1_result.json could be written).
        last_tick_metrics: dict[str, Any] = {}
        os.environ.pop("PB1_PENDING_RECONCILE_ONLY", None)
        while True:
            if stop_requested["value"]:
                exit_reason = "sigterm"
                break
            now = _get_now_kst()
            _touch_session_guard(
                runs_repo=runs_repo,
                session_guard_run_id=session_guard_run_id,
                session_kind=session_kind,
            )

            if now >= session_end_dt:
                logger.info(
                    "[PB1][LOOP] session end reached -> exit kind=%s now=%s session_end=%s",
                    session_kind,
                    now.isoformat(),
                    session_end_dt.isoformat(),
                )
                exit_reason = "session_end"
                break
            
            if _run_nontrading_smoke_if_needed(
                runtime_root_dir=runtime_root_dir,
                engine=engine,
                now=now,
                strategy_mode=strategy_mode,
            ):
                exit_reason = "nontrading_smoke_done"
                break

            window = decide_window(now=now, override=args.window)
            if window is None:
                sleep_for = _sleep_until_next_tick_or_session_end(now, session_end_dt, loop_interval)
                if sleep_for <= 0:
                    exit_reason = "session_end"
                    break
                logger.info(
                    "[PB1][LOOP] outside active window but session alive -> sleep %.0fs until next tick (session_end=%s)",
                    sleep_for,
                    session_end_dt.isoformat(),
                )
                time_mod.sleep(sleep_for)
                continue

            remaining_budget_s = max(0, int((session_end_dt - now).total_seconds()))
            min_tick_budget_sec = int(os.getenv("PB1_MIN_TICK_BUDGET_SEC", "105"))
            if remaining_budget_s < min_tick_budget_sec:
                logger.info(
                    "[PB1][LOOP] remaining_budget_too_small -> exit without new tick kind=%s remaining=%.0fs min_budget=%s session_end=%s",
                    session_kind,
                    remaining_budget_s,
                    min_tick_budget_sec,
                    session_end_dt.isoformat(),
                )
                logger.info("[PB1][EXIT] reason=session_end")
                exit_reason = "session_end"
                break
            grace_sec = _resolve_session_exit_grace_sec()
            base_tick_timeout = max(5, _parse_int_env("PB1_TICK_HARD_TIMEOUT_SEC", 90))
            remaining_to_session_end = max(0, int((session_end_dt - now).total_seconds()))
            tick_timeout_sec = min(
                base_tick_timeout,
                max(5, remaining_to_session_end + grace_sec),
            )
            try:
                tick_index += 1
                ticks_total += 1
                os.environ["PB1_LOOP_TICK_INDEX"] = str(tick_index)
                logger.info(
                    "[PB1][TICK][CALL_RUN_ONCE] kind=%s now=%s session_end=%s",
                    session_kind,
                    now.isoformat(),
                    session_end_dt.isoformat(),
                )
                _touched, _did_work, metrics, last_phase, result_status = _run_once_with_hard_timeout(
                    timeout_sec=tick_timeout_sec,
                    call=lambda: run_once(
                        args=args,
                        engine=engine,
                        ctx=ctx,
                        loop_mode=True,
                        window=window,
                        max_seconds=remaining_budget_s,
                        runs_ledger_fail_open=runs_ledger_fail_open,
                    ),
                )
                last_tick_metrics = dict(metrics or {})
                session_metrics["ticks_total"] += 1
                session_metrics["buy_orders"] += int(last_tick_metrics.get("buy_orders", 0) or 0)
                session_metrics["sell_orders"] += int(last_tick_metrics.get("sell_orders", 0) or 0)
                tick_buy_ack = int(last_tick_metrics.get("buy_orders_ack", last_tick_metrics.get("accepted", 0)) or 0)
                session_metrics["buy_orders_ack"] += tick_buy_ack
                session_metrics["sell_orders_ack"] += int(last_tick_metrics.get("sell_orders_ack", last_tick_metrics.get("sell_orders", 0)) or 0)
                session_metrics["order_candidates"] += int(last_tick_metrics.get("order_candidates", last_tick_metrics.get("order_candidate_count", 0)) or 0)
                session_metrics["api_submitted"] += int(last_tick_metrics.get("api_submitted", last_tick_metrics.get("submitted", last_tick_metrics.get("submit_success_count", 0))) or 0)
                # Broker ACK is not a fill confirmation; fills use the separate
                # reconciled ``filled`` counter below.
                session_metrics["accepted"] += int(last_tick_metrics.get("accepted", tick_buy_ack) or 0)
                session_metrics["rejected"] += int(last_tick_metrics.get("rejected", 0) or 0)
                session_metrics["skipped"] += int(last_tick_metrics.get("skipped", last_tick_metrics.get("skipped_count", 0)) or 0)
                session_metrics["filled_confirmed"] += int(last_tick_metrics.get("filled", 0) or 0)
                logger.info(
                    "[PB1][TICK][DONE] kind=%s now=%s result_status=%s",
                    session_kind,
                    _get_now_kst().isoformat(),
                    result_status,
                )
                last_result_status = result_status
                _touch_session_guard(
                    runs_repo=runs_repo,
                    session_guard_run_id=session_guard_run_id,
                    session_kind=session_kind,
                )
                balance_api_calls += metrics.get("balance_api_calls", 0)
                balance_cache_hits += metrics.get("balance_cache_hits", 0)
                balance_tick_cache_hits += metrics.get("balance_tick_cache_hits", 0)
                buy_orders += int(metrics.get("buy_orders", 0) or 0)
                sell_orders += int(metrics.get("sell_orders", 0) or 0)
                skip_now, keep_pending = _resolve_reconcile_only_followup(
                    enabled=str(os.getenv("PB1_RECONCILE_ONLY_AFTER_ORDER_SUBMIT") or "0") == "1",
                    pending=str(os.getenv("PB1_PENDING_RECONCILE_ONLY") or "0") == "1",
                    buy_orders=int(metrics.get("buy_orders", 0) or 0),
                    sell_orders=int(metrics.get("sell_orders", 0) or 0),
                    open_orders_count=0,
                )
                if not skip_now:
                    if keep_pending:
                        os.environ["PB1_PENDING_RECONCILE_ONLY"] = "1"
                    else:
                        os.environ.pop("PB1_PENDING_RECONCILE_ONLY", None)
                session_warning_counts = _merge_warning_counts(session_warning_counts, metrics.get("warning_counts"))
                if bool(metrics.get("degraded", False)) or str(result_status or "").startswith("DEGRADED"):
                    ticks_degraded += 1
                elif result_status in {"NO_TRADE", "OK_NO_TRADE", "OK_NO_CANDIDATES", "HANDOFF_TO_CLOSE", "NONTRADING_SMOKE_DONE"}:
                    ticks_no_trade += 1
                elif str(result_status or "").startswith("FATAL"):
                    ticks_fatal += 1
                else:
                    ticks_ok += 1
                now_after_tick = _get_now_kst()
                if now_after_tick >= session_end_dt:
                    logger.info(
                        "[PB1][LOOP][SESSION_END_RELEASE] kind=%s now=%s session_end=%s result_status=%s",
                        session_kind,
                        now_after_tick.isoformat(),
                        session_end_dt.isoformat(),
                        result_status,
                    )
                    exit_reason = "session_end"
                    break
                if result_status == "NONTRADING_SMOKE_DONE":
                    logger.info("[PB1][LOOP] non-trading-day exit status=%s", result_status)
                    exit_reason = "nontrading_smoke_done"
                    break
                if result_status == "HANDOFF_TO_CLOSE":
                    if now_after_tick >= session_end_dt:
                        logger.info(
                            "[PB1][LOOP] handoff accepted at session boundary kind=%s now=%s session_end=%s",
                            session_kind,
                            now_after_tick.isoformat(),
                            session_end_dt.isoformat(),
                        )
                        exit_reason = "session_end"
                        break
                    sleep_for = _sleep_until_next_tick_or_session_end(now_after_tick, session_end_dt, loop_interval)
                    logger.info(
                        "[PB1][LOOP] handoff requested before session end -> defer continue sleep=%.0fs now=%s session_end=%s",
                        sleep_for,
                        now_after_tick.isoformat(),
                        session_end_dt.isoformat(),
                    )
                    if sleep_for <= 0:
                        exit_reason = "session_end"
                        break
                    time_mod.sleep(sleep_for)
                    sticky_precheck_reason = None
                    sticky_precheck_count = 0
                    sticky_runtime_signature = None
                    sticky_runtime_count = 0
                    continue
                if result_status in {"NO_TRADE", "OK_NO_TRADE", "OK_NO_CANDIDATES"}:
                    sleep_for = _sleep_until_next_tick_or_session_end(_get_now_kst(), session_end_dt, loop_interval)
                    logger.info(
                        "[PB1][LOOP] no-trade tick only -> continue result_status=%s sleep=%.0fs session_end=%s",
                        result_status,
                        sleep_for,
                        session_end_dt.isoformat(),
                    )
                    if sleep_for <= 0:
                        exit_reason = "session_end"
                        break
                    time_mod.sleep(sleep_for)
                    sticky_precheck_reason = None
                    sticky_precheck_count = 0
                    sticky_runtime_signature = None
                    sticky_runtime_count = 0
                    continue
                sticky_precheck_reason = None
                sticky_precheck_count = 0
                sticky_runtime_signature = None
                sticky_runtime_count = 0
                sleep_for = _sleep_until_next_tick_or_session_end(_get_now_kst(), session_end_dt, loop_interval)
                if sleep_for <= 0:
                    exit_reason = "session_end"
                    break
                time_mod.sleep(sleep_for)
                continue
            except TickTimeoutError as exc:
                # RECOVERABLE TIMEOUT - not fatal, just degraded
                ticks_degraded += 1
                session_warning_counts["timeout_count"] = int(session_warning_counts.get("timeout_count", 0)) + 1
                session_warning_counts["tick_timeout_recoverable"] = int(session_warning_counts.get("tick_timeout_recoverable", 0)) + 1
                
                _last_stage_on_timeout = os.getenv("PB1_LAST_STAGE", "unknown")
                logger.warning(
                    "[PB1][TICK_TIMEOUT][RECOVERABLE] kind=%s tick=%s last_stage=%s timeout_sec=%s now=%s session_end=%s",
                    session_kind,
                    ticks_total + 1,
                    _last_stage_on_timeout,
                    tick_timeout_sec,
                    now.isoformat(),
                    session_end_dt.isoformat(),
                )
                
                # tick timeout 후 오염된 DB connection dispose
                if os.getenv("PB1_DISPOSE_ENGINE_ON_TICK_TIMEOUT", "1") not in {"0", "false"}:
                    dispose_engine_safely(engine, reason=f"tick_hard_timeout:{_last_stage_on_timeout}")
                    session_warning_counts["db_engine_dispose_count"] = int(session_warning_counts.get("db_engine_dispose_count", 0)) + 1
                    logger.warning(
                        "[PB1][TICK][TIMEOUT][DB_DISPOSE] last_stage=%s",
                        _last_stage_on_timeout,
                    )
                    # recovery sleep
                    _recovery_sleep_sec = max(1, int(os.getenv("PB1_DB_RECOVERY_SLEEP_SEC", "2")))
                    logger.info("[PB1][TICK][RECOVERY_SLEEP] sec=%s reason=db_timeout", _recovery_sleep_sec)
                    time_mod.sleep(_recovery_sleep_sec)
                
                now_after_timeout = _get_now_kst()
                if now_after_timeout >= session_end_dt:
                    logger.info("[PB1][LOOP] after timeout recovery, session_end reached -> exit")
                    exit_reason = "session_end"
                    break
                sleep_for = _sleep_until_next_tick_or_session_end(
                    now_after_timeout,
                    session_end_dt,
                    min(loop_interval, 5),
                )
                if sleep_for <= 0:
                    exit_reason = "session_end"
                    break
                time_mod.sleep(sleep_for)
                continue
            except Exception as exc:
                message = str(exc)
                if message.startswith("ENTRY_ABORT_PRECHECK:"):
                    precheck_reason = message.split(":", 1)[1]
                    if sticky_precheck_reason == precheck_reason:
                        sticky_precheck_count += 1
                    else:
                        sticky_precheck_reason = precheck_reason
                        sticky_precheck_count = 1
                    logger.error(
                        "[PB1][PRECHECK][FATAL] reason=%s consecutive=%s",
                        precheck_reason,
                        sticky_precheck_count,
                    )
                    if "db_schema_mismatch_fills_filled_at" in precheck_reason and sticky_precheck_count >= 2:
                        exit_reason = "STRUCTURAL_FATAL"
                        logger.error("[PB1][LOOP][STRUCTURAL_FATAL] reason=%s -> exit loop immediately", precheck_reason)
                        break
                    if sticky_precheck_count >= 2:
                        exit_reason = "PRECHECK_FATAL_STICKY"
                        logger.error("[PB1][PRECHECK][STICKY] reason=%s -> exit loop", precheck_reason)
                        break

                structural_fatal_tokens = (
                    "db_exact_scored_final30_invalid",
                    "strategy_mismatch",
                    "final30_contract_invalid",
                    "db_only_universe_missing",
                    "FINAL30_INPUT_ROWS_MISMATCH",
                    "ASOF_INVARIANT_VIOLATION",
                    "locked_final30_source_mismatch",
                    "missing_db_exact_scored_final30",
                )

                if any(token in message for token in structural_fatal_tokens):
                    logger.error(
                        "[PB1][LOOP][STRUCTURAL_FATAL] reason=%s -> exit loop immediately",
                        message,
                    )
                    exit_reason = "STRUCTURAL_FATAL"
                    break

                repeat_threshold = max(2, _parse_int_env("PB1_RUNTIME_FATAL_REPEAT_THRESHOLD", 2))
                sticky_runtime_signature, sticky_runtime_count, should_stop_for_repeat = _update_runtime_fatal_guard(
                    previous_signature=sticky_runtime_signature,
                    previous_count=sticky_runtime_count,
                    exc=exc,
                    repeat_threshold=repeat_threshold,
                )
                logger.error(
                    "[PB1][FATAL_SIGNATURE] name=%s message=%s repeat=%s",
                    sticky_runtime_signature[0],
                    sticky_runtime_signature[1],
                    sticky_runtime_count,
                )
                if should_stop_for_repeat:
                    exit_reason = "FATAL_RUNTIME_REPEAT"
                    logger.error(
                        "[PB1][LOOP][FATAL_REPEAT] action=stop_entry_only name=%s repeat=%s -> exit loop",
                        sticky_runtime_signature[0],
                        sticky_runtime_count,
                    )
                    break

                logger.error(
                    "[PB1][TICK][FATAL_GUARD] exception=%s\n%s",
                    exc,
                    traceback.format_exc(),
                )
                ticks_fatal += 1
                sleep_for = _sleep_until_next_tick_or_session_end(_get_now_kst(), session_end_dt, min(loop_interval, 3))
                if sleep_for <= 0:
                    exit_reason = "session_end"
                    break
                time_mod.sleep(sleep_for)
        elapsed_trade = time_mod.monotonic() - loop_started_ts
        elapsed_total = time_mod.monotonic() - total_start_ts
        if exit_reason == "unknown":
            exit_reason = "shutdown"
        logger.info(
            "[PB1][EXIT] reason=%s elapsed_trade=%.1fs elapsed_total=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
            exit_reason,
            elapsed_trade,
            elapsed_total,
            max_seconds,
            loop_deadline.isoformat(),
            last_phase,
            balance_api_calls,
            balance_cache_hits,
            balance_tick_cache_hits,
        )
        logger.info(
            "[PB1][SESSION_SUMMARY] kind=%s ticks_total=%s ticks_ok=%s ticks_no_trade=%s ticks_fatal=%s ticks_degraded=%s buy_orders=%s sell_orders=%s",
            session_kind,
            ticks_total,
            ticks_ok,
            ticks_no_trade,
            ticks_fatal,
            ticks_degraded,
            buy_orders,
            sell_orders,
        )
        # close phase에서 tick이 0개이면 명시 경고
        _forced_phase = (os.getenv("FORCE_PB1_PHASE") or os.getenv("PB1_PHASE_DEFAULT") or "").strip().lower()
        if ticks_total == 0 and _forced_phase == "exit":
            _close_session_end = os.getenv("PB1_CLOSE_SESSION_END", "15:30")
            logger.warning(
                "[TRADE_CLOSE][NO_TICK][WARN] reason=no_ticks_executed now=%s close_session_end=%s exit_reason=%s close_ticks_total=0 close_executed=0",
                _get_now_kst().strftime("%H%M%S"),
                _close_session_end.replace(":", ""),
                exit_reason,
            )
        logger.info(
            "[PB1][SESSION_WARNINGS] timeout_count=%s db_read_fail_open_count=%s ledger_fail_first_count=%s degraded_stage_count=%s duplicate_skip_count=%s db_engine_dispose_count=%s",
            session_warning_counts.get("timeout_count", 0),
            session_warning_counts.get("db_read_fail_open_count", 0),
            session_warning_counts.get("ledger_fail_first_count", 0),
            session_warning_counts.get("degraded_stage_count", 0),
            session_warning_counts.get("duplicate_skip_count", 0),
            session_warning_counts.get("db_engine_dispose_count", 0),
        )
        logger.info(
            "[PB1][SESSION_TERMINAL] terminal_state=%s result_status=%s exit_reason=%s warnings_total=%s",
            _resolve_session_terminal_state(
                result_status=last_result_status,
                exit_reason=exit_reason,
                warning_counts=session_warning_counts,
            ),
            last_result_status,
            exit_reason,
            _warning_total(session_warning_counts),
        )
        os.environ["PB1_LAST_RESULT_STATUS"] = str(last_result_status)
        os.environ["PB1_LAST_EXIT_REASON"] = str(exit_reason)
        marker_metrics = session_metrics
        status_for_marker = str(last_result_status or "")
        if int(session_metrics.get("api_submitted", 0) or 0) > 0 and status_for_marker.upper() == "OK_NO_TRADE":
            status_for_marker = "OK_WITH_ORDERS"
        normalized = normalize_session_result(
            status=status_for_marker,
            reason=exit_reason,
            order_candidates=int(marker_metrics.get("order_candidates", marker_metrics.get("order_candidate_count", 0)) or 0),
            api_submitted=int(marker_metrics.get("api_submitted", marker_metrics.get("submitted", marker_metrics.get("submit_success_count", 0))) or 0),
            skipped=int(marker_metrics.get("skipped", marker_metrics.get("skipped_count", 0)) or 0),
            skip_reasons=marker_metrics.get("skip_reasons") or [],
        )
        _record_session_execution_marker(
            engine=engine,
            env=ctx.env,
            session_kind=session_kind,
            trade_date=_get_now_kst().date(),
            status=normalized.status,
            exit_reason=normalized.exit_reason,
            recovery_used=str(os.getenv("PB1_SESSION_RECOVERY_USED") or "0") == "1",
            completed=bool(normalized.completed),
            retryable=bool(normalized.retryable),
            sell_orders_ack=int(sell_orders or 0),
            entry_status="DONE" if normalized.completed else "ABORT",
            entry_abort_reason=None if normalized.completed else normalized.exit_reason,
            exit_code=0 if normalized.completed else 1,
        )
        _write_session_result_file({
            "status": status_for_marker,
            "exit_reason": exit_reason,
            "ticks_total": int(session_metrics.get("ticks_total", ticks_total) or 0),
            "sell_orders_ack": int(session_metrics.get("sell_orders_ack", sell_orders) or 0),
            "sell_orders": int(session_metrics.get("sell_orders", sell_orders) or 0),
            "buy_orders": int(session_metrics.get("buy_orders", buy_orders) or 0),
            "buy_orders_ack": int(session_metrics.get("buy_orders_ack", 0) or 0),
            "order_candidates": int(session_metrics.get("order_candidates", 0) or 0),
            "api_submitted": int(session_metrics.get("api_submitted", 0) or 0),
            "accepted": int(session_metrics.get("accepted", 0) or 0),
            "filled_confirmed": int(session_metrics.get("filled_confirmed", 0) or 0),
            "rejected": int(session_metrics.get("rejected", 0) or 0),
            "skipped": int(session_metrics.get("skipped", 0) or 0),
        })
    finally:
        _finish_session_guard(
            runs_repo=runs_repo,
            session_guard_run_id=session_guard_run_id,
            session_kind=session_kind,
            exit_reason=exit_reason,
            last_phase=last_phase,
        )
        try:
            release_advisory_xact_lock(lock_conn, context=lock_context)
        except Exception as exc:
            logger.warning("[PB1][LOCK][RELEASE_FAIL] advisory lock release failed (ignoring): %s", exc)
        try:
            lock_conn.close()
        except Exception as exc:
            logger.warning("[PB1][LOCK][CLOSE_FAIL] lock connection close failed (ignoring): %s", exc)


def _exit_code_for_status(status: str) -> int:
    if status in {"FAIL_PRECHECK", "DB_EXACT_FINAL30_ZERO", "FINAL30_ZERO"}:
        return 2
    if status in {"FAILED", "ERROR", "FATAL_RUNTIME"}:
        return 1
    return 0


def _log_main_run_summary(status: str, reason: str) -> None:
    logger.info(
        "[RUN_SUMMARY][RESULT] status=%s reason=%s session=%s event=%s",
        status,
        reason,
        str(os.getenv("PB1_SESSION_KIND") or os.getenv("FORCE_MARKET_WINDOW") or "unknown"),
        str(os.getenv("GITHUB_EVENT_NAME") or "unknown"),
    )


def main() -> int:
    args = parse_args()
    logger.info("[TRADE][BOOT][START]")
    resolved_env = resolve_env(getattr(args, "env", None))
    os.environ["STRATEGY_ENV"] = resolved_env
    if not os.getenv("KIS_ENV"):
        os.environ["KIS_ENV"] = resolved_env
    strategy_env_raw = os.getenv("STRATEGY_ENV")
    kis_env_raw = os.getenv("KIS_ENV")
    derived_env = (strategy_env_raw or kis_env_raw or "practice").strip().lower()
    logger.info(
        "[TRADE][BOOT][ENV] MODE=%s STRATEGY_ENV=%s KIS_ENV=%s FAIL_IF_POOL_MISSING=%s CANDIDATE_POOL_STRATEGY_KEY=%s PB1_PHASE_DEFAULT=%s",
        os.getenv("MODE", "trade"),
        strategy_env_raw or "",
        kis_env_raw or "",
        os.getenv("FAIL_IF_POOL_MISSING", ""),
        os.getenv("CANDIDATE_POOL_STRATEGY_KEY", ""),
        os.getenv("PB1_PHASE_DEFAULT", ""),
    )

    # ✅ 설계 1: JOB 모드 분리 (BUILD_WATCHLIST vs TRADE_INTRADAY)
    job_mode = os.getenv("PB1_JOB", "TRADE_INTRADAY").upper()
    
    if job_mode == "BUILD_WATCHLIST":
        logger.info("[PB1][JOB] mode=BUILD_WATCHLIST -> build weekly watchlist and exit")
        return _run_build_watchlist_job()
    
    # else: TRADE_INTRADAY (기존 로직)
    logger.info("[PB1][JOB] mode=TRADE_INTRADAY -> run entry/exit logic")

    mode_input = (os.getenv("MODE") or "").strip().lower()
    minervini_test = mode_input == "minervini_test"
    minervini_with_pb1 = env_bool("MINERVINI_TEST_WITH_PB1", default=False)
    if minervini_test:
        selection_only = not minervini_with_pb1
        if selection_only:
            os.environ["MINERVINI_ONLY"] = "1"
        else:
            os.environ["MINERVINI_ONLY"] = "0"
        logger.info(
            "[RUN_PLAN] MODE=minervini_test selection_only=%s with_pb1=%s",
            int(selection_only),
            int(minervini_with_pb1),
        )
    
    # ✅ DIAG Minervini-only 모드 체크 (최우선 처리)
    diag_minervini_only = os.getenv("PB1_DIAG_MINERVINI_ONLY", "0") == "1"
    force_candidate_pool = os.getenv("PB1_FORCE_CANDIDATE_POOL", "0") == "1"
    strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
    
    if diag_minervini_only and strategy_mode == "DIAG":
        logger.info(
            "[PB1][DIAG_MINERVINI_ONLY] mode=DIAG diag_minervini_only=1 -> run minervini filter only and exit"
        )
        
        # DB 준비
        assert_db_ready()
        engine = make_engine()
        _ensure_bootstrap_migrations(engine)
        
        # 환경변수
        env = resolved_env
        strategy = os.getenv("STRATEGY", "best_k_meta")
        today = now_kst().date()
        
        logger.info(
            "[PB1][DIAG_MINERVINI_ONLY] env=%s strategy=%s as_of=%s force_pool=%s",
            env, strategy, today, force_candidate_pool
        )
        
        # Minervini 실행
        from trader.minervini_runner import run_diag_minervini_only
        
        try:
            exit_code = run_diag_minervini_only(
                engine=engine,
                env=env,
                strategy=strategy,
                as_of=today,
            )
            logger.info(
                "[PB1][DIAG_MINERVINI_ONLY][EXIT] code=%s reason=minervini_only_complete",
                exit_code
            )
            return exit_code
        except Exception as exc:
            logger.exception("[PB1][DIAG_MINERVINI_ONLY][FAIL] %s", exc)
            return 1
    
    # ✅ 환경변수 검증: KIS_ENV vs STRATEGY_ENV 일치 확인
    kis_env_raw = os.getenv("KIS_ENV")
    strategy_env_raw = os.getenv("STRATEGY_ENV")
    kis_env = (kis_env_raw or "").strip().lower()
    strategy_env = (strategy_env_raw or "").strip().lower()
    
    # ✅ NEW: allow mismatch in DIAG/DRY_RUN (candidate-only/minervini test)
    # LIVE real trading must remain strict.
    live_trading_enabled = env_bool("LIVE_TRADING_ENABLED", default=False)
    dry_run = env_bool("DRY_RUN", default=True)
    sim_mode = env_bool("SIM_MODE", default=False)
    pb1_candidate_only = env_bool("PB1_CANDIDATE_ONLY", default=False)
    diag_minervini_only = env_bool("PB1_DIAG_MINERVINI_ONLY", default=False)
    strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
    
    # 허용 조건: DIAG, DRY_RUN, SIM_MODE, candidate-only 중 하나라도 활성화
    allow_mismatch = (
        (strategy_mode == "DIAG") or
        dry_run or
        sim_mode or
        pb1_candidate_only or
        diag_minervini_only
    )
    
    if kis_env and strategy_env and kis_env != strategy_env:
        raise RuntimeError("ENV_MISMATCH_IN_TRADE")
    
    # ✅ KIS_ENV가 없으면 STRATEGY_ENV로 설정
    if not kis_env and strategy_env:
        os.environ["KIS_ENV"] = strategy_env
        logger.info("[PB1][ENV][AUTO] KIS_ENV not set -> using STRATEGY_ENV=%s", strategy_env)
    
    # ✅ STRATEGY_ENV가 없으면 KIS_ENV로 설정
    if not strategy_env and kis_env:
        os.environ["STRATEGY_ENV"] = kis_env
        logger.info("[PB1][ENV][AUTO] STRATEGY_ENV not set -> using KIS_ENV=%s", kis_env)

    strategy_env_raw = os.getenv("STRATEGY_ENV")
    kis_env_raw = os.getenv("KIS_ENV")
    derived_env = (strategy_env_raw or kis_env_raw or derived_env or "practice").strip().lower()
    
    # ✅ [NEW] MINERVINI_ONLY 모드 강제 설정
    minervini_only_env = os.getenv("MINERVINI_ONLY", "0") == "1"
    if minervini_only_env:
        os.environ["KIS_HTTP_ENABLED"] = "0"
        os.environ["DISABLE_LIVE_TRADING"] = "1"
        os.environ["LIVE_TRADING_ENABLED"] = "0"
        os.environ["STRATEGY_MODE"] = "DIAG"
        os.environ["PB1_PHASE_DEFAULT"] = "entry"
        logger.warning(
            "[MINERVINI_ONLY] force DIAG + KIS_HTTP_ENABLED=0 + PB1_PHASE_DEFAULT=entry"
        )
    
    close_manual_mode = _activate_close_manual_replay_env()

    # ✅ AUTO 모드 결정 및 환경변수 고정
    mode_env = os.getenv("STRATEGY_MODE", "AUTO")
    resolved_mode = resolve_auto_strategy_mode(mode_env)
    os.environ["STRATEGY_MODE"] = resolved_mode
    logger.info(
        "[PB1][MODE] mode_env=%s resolved=%s fixed_in_env=True MINERVINI_ONLY=%s",
        mode_env,
        resolved_mode,
        int(minervini_only_env),
    )

    # ✅ Trade guard: PREP_DONE + derived_minervini + today watchlist required
    if mode_input == "trade":
        os.environ["PB1_TRADE_WATCHLIST_ONLY"] = "1"

        trade_asof_source = (os.getenv("TRADE_ASOF_SOURCE") or "").strip().lower()
        as_of_override_raw = (os.getenv("AS_OF_OVERRIDE") or "").strip()
        allow_wl_only = os.getenv("ALLOW_TRADE_WITH_WATCHLIST_ONLY", "0") == "1"
        trade_run_minervini = os.getenv("TRADE_RUN_MINERVINI", "0") == "1"

        if trade_run_minervini:
            os.environ["PB1_DIAG_FULL_EXEC"] = "1"
            os.environ["FORCE_PB1_PHASE"] = "entry"
            logger.info(
                "[TRADE][MINERVINI] TRADE_RUN_MINERVINI=1 -> force PB1_DIAG_FULL_EXEC=%s FORCE_PB1_PHASE=%s",
                os.getenv("PB1_DIAG_FULL_EXEC"),
                os.getenv("FORCE_PB1_PHASE"),
            )

        now = now_kst()
        trade_date = now.date()
        watchlist_loaded_for_guard = False

        def _ensure_watchlist_for_guard(requested_as_of):
            nonlocal watchlist_loaded_for_guard
            watchlist_engine = make_engine()
            watchlist_env = resolve_env(args.env)
            df = _hydrate_locked_final30_from_db_only(
                engine=watchlist_engine,
                env=watchlist_env,
                as_of=requested_as_of.isoformat(),
            )

            watchlist_loaded_for_guard = True
            rows = [dict(x) for x in df.to_dict(orient="records")]
            top10_codes = [str(item.get("code") or "").zfill(6) for item in rows[:10] if item.get("code")]
            logger.info(
                "[TRADE][WATCHLIST_FINAL][LOCK] env=%s strategy=%s requested_as_of=%s actual_as_of=%s n=%s",
                watchlist_env,
                os.getenv("WATCHLIST_FINAL_STRATEGY_KEY", "pb1_watchlist_final_scored").strip().lower(),
                requested_as_of.isoformat(),
                requested_as_of.isoformat(),
                len(rows),
            )
            logger.info("[TRADE][WATCHLIST_FINAL][TOP10] codes=%s", top10_codes)
            return requested_as_of

        if as_of_override_raw:
            derived_as_of = _parse_as_of_override(as_of_override_raw)
            asof_reason = "AS_OF_OVERRIDE"
            if allow_wl_only:
                _ensure_watchlist_for_guard(derived_as_of)
        elif trade_asof_source == "watchlist":
            watchlist_as_of = _ensure_watchlist_for_guard(trade_date)
            derived_as_of = watchlist_as_of
            watchlist_reason = "exact" if watchlist_as_of == trade_date else "ttl_fallback"
            asof_reason = f"WATCHLIST_ASOF({watchlist_reason})"
        else:
            # ✅ CRITICAL: 장중 매매는 "전일 영업일 derived"를 사용해야 함
            # prep_runner가 전일 종가로 derived를 생성하므로, trade는 전일을 참조
            derived_as_of = resolve_derived_as_of(now)
            asof_reason = "INTRADAY_USE_PREV_CLOSE"

        # trade 실행 전 구간(run_once/engine 포함) 전체에서 동일 as_of를 강제
        os.environ["AS_OF_OVERRIDE"] = derived_as_of.isoformat()
        
        logger.info(
            "[ASOF][TRADE] trade_date=%s derived_as_of=%s reason=%s",
            trade_date.isoformat(),
            derived_as_of.isoformat(),
            asof_reason,
        )
        
        engine = make_engine()
        ledger_repo = LedgerEventsRepo(engine)
        derived_env = (os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice").strip().lower()
        watchlist_repo = WatchlistRepo(engine)
        prep_done, prep_done_source, prep_done_count, prep_done_details = _resolve_trade_prep_done_status(
            ledger_repo=ledger_repo,
            watchlist_repo=watchlist_repo,
            env=derived_env,
            strategy="pb1",
            as_of=derived_as_of,
            trade_date=trade_date,
        )
        if not prep_done:
            logger.warning("[TRADE_TICK][SKIP] reason=PREP_NOT_DONE")
            _log_main_run_summary(status="SKIP_PRECHECK", reason="PREP_NOT_DONE")
            return 0
        
        # DERIVED 체크도 derived_as_of 기준으로
        derived_repo = DerivedMinerviniRepo(engine)
        derived_count = derived_repo.count_as_of(env=derived_env, as_of=derived_as_of)
        logger.info(
            "[PB1][ENV_DERIVE] strategy_env=%s kis_env=%s derived_env=%s",
            os.getenv("STRATEGY_ENV"),
            os.getenv("KIS_ENV"),
            derived_env,
        )
        watchlist_final_count = watchlist_repo.count_watchlist(
            env=derived_env,
            strategy="pb1_watchlist_final",
            as_of=derived_as_of,
        )
        watchlist_scored_count = watchlist_repo.count_watchlist(
            env=derived_env,
            strategy="pb1_watchlist_final_scored",
            as_of=derived_as_of,
        )
        scored_contract = watchlist_repo.verify_watchlist_scored_contract(
            env=derived_env,
            as_of=derived_as_of,
            strategy="pb1_watchlist_final_scored",
            allow_latest_fallback=False,
        )
        scored_missing_critical = [
            col for col in REQUIRED_FINAL30_SCORED_COLS
            if col not in [str(item) for item in (scored_contract.get("columns") or [])]
        ]
        scored_contract_repairable = _is_trade_repairable_db_contract(
            scored_contract,
            missing_critical_fields=scored_missing_critical,
        )
        plain_final_ok = bool(watchlist_final_count == 30)
        scored_final_ok = bool(
            watchlist_scored_count == 30
            and not scored_missing_critical
            and (scored_contract.get("ok") or scored_contract_repairable)
        )
        repairable_plain_from_scored = bool(
            (not plain_final_ok)
            and scored_final_ok
        )
        logged_watchlist_final_count = watchlist_final_count
        logged_watchlist_scored_count = watchlist_scored_count
        if repairable_plain_from_scored:
            scored_rows_data = list(scored_contract.get("rows_data") or [])
            plain_backfill_members = []
            for idx, row in enumerate(scored_rows_data, start=1):
                normalized = normalize_final30_contract_row(dict(row or {}))
                code = str(normalized.get("code") or "").zfill(6)
                if not code:
                    continue
                plain_backfill_members.append(
                    {
                        "code": code,
                        "rank": int(normalized.get("rank") or normalized.get("rank_final30") or idx),
                        "score": normalized.get("score_final", normalized.get("final_score", normalized.get("score"))),
                        "meta": dict(normalized.get("meta") or {}),
                    }
                )

            if plain_backfill_members:
                watchlist_repo.save_watchlist(
                    env=derived_env,
                    strategy="pb1_watchlist_final",
                    as_of=derived_as_of,
                    members=plain_backfill_members,
                )
                watchlist_final_count = watchlist_repo.count_watchlist(
                    env=derived_env,
                    strategy="pb1_watchlist_final",
                    as_of=derived_as_of,
                )
                plain_final_ok = bool(watchlist_final_count == 30)
                logger.warning(
                    "[TRADE_TICK][DB_CONTRACT][BACKFILL_PLAIN_FROM_SCORED] env=%s as_of=%s repaired_rows=%s plain_final_ok=%s",
                    derived_env,
                    derived_as_of.isoformat(),
                    len(plain_backfill_members),
                    int(plain_final_ok),
                )
        db_contract_ok = bool(
            prep_done
            and derived_count > 0
            and (plain_final_ok or scored_final_ok)
        )
        logger.info(
            "[TRADE_TICK][DB_CONTRACT] env=%s as_of=%s prep_done=%s derived_minervini=%s watchlist_final=%s watchlist_final_scored=%s plain_ok=%s scored_ok=%s repairable_plain_from_scored=%s db_contract_ok=%s",
            derived_env,
            derived_as_of.isoformat(),
            int(prep_done),
            derived_count,
            logged_watchlist_final_count,
            logged_watchlist_scored_count,
            int(logged_watchlist_final_count == 30),
            int(scored_final_ok),
            int(repairable_plain_from_scored),
            int(db_contract_ok),
        )
        logger.info(
            "[TRADE][PRECHECK][CANDIDATE_POOL] status=%s derived_count=%s watchlist_final=%s watchlist_scored=%s",
            "OK" if db_contract_ok else "FAIL",
            derived_count,
            watchlist_final_count,
            watchlist_scored_count,
        )
        if not db_contract_ok:
            logger.error("[TRADE][READY][FAIL] reason=candidate_pool_or_db_contract_invalid")
            logger.warning(
                "[TRADE_TICK][SKIP] reason=DB_CONTRACT_INCOMPLETE derived_as_of=%s trade_date=%s plain_rows=%s scored_rows=%s uniq_codes=%s uniq_ranks=%s null_critical=%s missing_critical=%s",
                derived_as_of.isoformat(),
                trade_date.isoformat(),
                watchlist_final_count,
                watchlist_scored_count,
                scored_contract.get("uniq_codes"),
                scored_contract.get("uniq_ranks"),
                scored_contract.get("null_critical"),
                scored_missing_critical,
            )
            _log_main_run_summary(status="SKIP_PRECHECK", reason="DB_CONTRACT_INCOMPLETE")
            return 0
        
        # ✅ FALLBACK: 전일 derived 없으면 최근 영업일로 fallback
        if derived_count <= 0:
            if allow_wl_only and watchlist_loaded_for_guard:
                logger.warning(
                    "[TRADE_TICK][DERIVED][BYPASS] derived_missing but watchlist_final locked -> continue derived_as_of=%s trade_date=%s",
                    derived_as_of.isoformat(),
                    trade_date.isoformat(),
                )
            elif DERIVED_FALLBACK_ENABLED:
                logger.info(
                    "[TRADE_TICK][FALLBACK][START] derived_as_of=%s missing -> trying fallback (max_back=%d ttl=%d)",
                    derived_as_of.isoformat(),
                    DERIVED_FALLBACK_MAX_DAYS,
                    CANDIDATE_POOL_TTL_DAYS,
                )
                
                fallback_as_of = derived_repo.find_latest_available_asof(
                    env=derived_env,
                    target_as_of=derived_as_of,
                    max_back_days=DERIVED_FALLBACK_MAX_DAYS,
                    ttl_days=CANDIDATE_POOL_TTL_DAYS,
                )
                
                if fallback_as_of:
                    age_days = (derived_as_of - fallback_as_of).days
                    
                    # ✅ 안전장치: fallback age가 크면 강력한 경고
                    if age_days >= DERIVED_FALLBACK_WARN_AGE_DAYS:
                        logger.error(
                            "[TRADE_TICK][FALLBACK][RISK_WARNING] ⚠️  STALE DATA RISK ⚠️  "
                            "derived_as_of=%s -> fallback=%s age=%d >= warn_threshold=%d | "
                            "데이터가 %d영업일 이상 오래되었습니다. 매매 리스크가 높습니다. "
                            "ANALYSIS_ONLY 권장 또는 수량 축소 고려",
                            derived_as_of.isoformat(),
                            fallback_as_of.isoformat(),
                            age_days,
                            DERIVED_FALLBACK_WARN_AGE_DAYS,
                            age_days,
                        )
                    else:
                        logger.warning(
                            "[TRADE_TICK][FALLBACK][SUCCESS] derived_as_of=%s -> fallback=%s age=%d (max_back=%d ttl=%d)",
                            derived_as_of.isoformat(),
                            fallback_as_of.isoformat(),
                            age_days,
                            DERIVED_FALLBACK_MAX_DAYS,
                            CANDIDATE_POOL_TTL_DAYS,
                        )
                    
                    # ✅ derived_as_of를 fallback으로 교체하여 이후 로직에서 사용
                    derived_as_of = fallback_as_of
                    derived_count = derived_repo.count_as_of(env=derived_env, as_of=derived_as_of)
                    
                    logger.info(
                        "[TRADE_TICK][FALLBACK][DERIVED][OK] fallback_as_of=%s count=%d age=%d",
                        derived_as_of.isoformat(),
                        derived_count,
                        age_days,
                    )
                else:
                    logger.warning(
                        "[TRADE_TICK][SKIP] reason=DERIVED_MISSING_FALLBACK_FAILED derived_as_of=%s trade_date=%s (max_back=%d ttl=%d)",
                        derived_as_of.isoformat(),
                        trade_date.isoformat(),
                        DERIVED_FALLBACK_MAX_DAYS,
                        CANDIDATE_POOL_TTL_DAYS,
                    )
                    _log_main_run_summary(status="SKIP_PRECHECK", reason="DERIVED_MISSING_FALLBACK_FAILED")
                    return 0
            else:
                logger.warning(
                    "[TRADE_TICK][SKIP] reason=DERIVED_MISSING derived_as_of=%s trade_date=%s count=%d (fallback_disabled)",
                    derived_as_of.isoformat(),
                    trade_date.isoformat(),
                    derived_count,
                )
                _log_main_run_summary(status="SKIP_PRECHECK", reason="DERIVED_MISSING")
                return 0
        else:
            # 전일 derived 존재
            logger.info(
                "[TRADE_TICK][DERIVED][OK] derived_as_of=%s count=%d",
                derived_as_of.isoformat(),
                derived_count,
            )
        
        # ✅ DIAGNOSTIC: canonical scored final30 존재 여부 체크
        logger.info("[TRADE][PRECHECK][FINAL30] status=START requested_as_of=%s", derived_as_of.isoformat())
        final30_df = _hydrate_locked_final30_from_db_only(
            engine=engine,
            env=derived_env,
            as_of=derived_as_of.isoformat(),
        )
        logger.info("[TRADE][PRECHECK][FINAL30] status=OK rows=%s", len(final30_df))
        os.environ["PB1_SKIP_NEW_ENTRIES"] = "0"

    assert_db_ready()
    smoke_enabled = os.getenv("PB1_SMOKE_RUN") == "1"
    run_loop_minutes, _max_minutes, max_seconds, loop_configured = _resolve_loop_limits()
    
    # ✅ PB1_LOOP_ENABLED 또는 loop_configured로 루프 활성화
    loop_enabled = os.getenv("PB1_LOOP_ENABLED", "0") == "1"
    run_loop = loop_enabled or os.getenv("PB1_RUN_LOOP", "0") == "1" or loop_configured
    
    # ✅ DIAG 모드에서는 루프 비활성화 (한 번만 실행)
    if resolved_mode == "DIAG":
        logger.info("[PB1][DIAG] mode=DIAG -> disable loop (run once)")
        run_loop = False

    if close_manual_mode is not None:
        close_now = now_kst()
        close_session_end = _resolve_session_end_dt(close_now)
        run_loop = False
        logger.info(
            "[TRADE_CLOSE][MANUAL_REPLAY] mode=%s now_kst=%s session_end=%s order_allowed=0 late_start=%s",
            close_manual_mode,
            close_now.isoformat(),
            close_session_end.isoformat(),
            int(close_now >= close_session_end),
        )
    
    if run_loop and not smoke_enabled:
        try:
            _run_loop(args=args)
        except Exception:
            logger.error("[PB1][FATAL_GUARD] loop crashed", exc_info=True)
        return 0
    engine = make_engine()
    if not hasattr(engine, "connect"):
        now = now_kst()
        open_dt, close_dt = _market_session(now)
        allow_wait = env_bool("PB1_ALLOW_WAIT", PB1_WAIT_FOR_WINDOW)
        max_wait_s = int(PB1_MAX_WAIT_FOR_WINDOW_MIN) * 60
        action, target_start = _decide_action(now, is_trading_day(now), open_dt, close_dt, allow_wait, max_wait_s, smoke_enabled)
        if action == "wait" and target_start is not None:
            remaining = max(0.0, (target_start - now).total_seconds())
            sleep_for = remaining if remaining < 30 else min(60, remaining)
            if sleep_for > 0:
                time_mod.sleep(sleep_for)
        logger.warning("[PB1][RUN] engine missing connect() -> exit")
        return 0
    # Dedicated lock-scope connection only; do not run order/fill/report writes
    # through it because release_advisory_xact_lock rolls its transaction back.
    lock_conn = engine.connect()
    lock_context = (
        f"market=KR mode=single "
        f"strategy_env={os.getenv('STRATEGY_ENV') or os.getenv('KIS_ENV') or os.getenv('ENV')} "
        f"session={os.getenv('PB1_SESSION_KIND') or os.getenv('SESSION_KIND') or os.getenv('FORCE_MARKET_WINDOW') or 'unknown'} "
        f"workflow={os.getenv('GITHUB_WORKFLOW')} "
        f"job={os.getenv('GITHUB_JOB')} "
        f"run_id={os.getenv('GITHUB_RUN_ID')} "
        f"run_attempt={os.getenv('GITHUB_RUN_ATTEMPT')} "
        f"trader_run_id={os.getenv('TRADER_RUN_ID')}"
    )
    if not try_acquire_lock(lock_conn, context=lock_context):
        logger.warning(
            "[PB1][RUN][SKIP_LOCKED] run lock unavailable owner_logged=1 action=safe_skip context=%s",
            lock_context,
        )
        os.environ["PB1_LAST_RESULT_STATUS"] = "SKIP_LOCKED"
        os.environ["PB1_LAST_EXIT_REASON"] = "PB1_ADVISORY_LOCK_UNAVAILABLE"
        _write_session_result_file({
            "status": "SKIP_LOCKED",
            "final_status": "SKIP_LOCKED",
            "exit_reason": "PB1_ADVISORY_LOCK_UNAVAILABLE",
            "engine_started": False,
            "pb1_result_present": False,
            "orders_intent": 0,
            "orders_ack": 0,
            "sell_orders_ack": 0,
            "retryable": True,
        })
        lock_conn.close()
        return 1
    _ensure_bootstrap_migrations(engine)
    _write_change_flag(False, ["init"])
    
    # ✅ run_id SSOT: TRADER_RUN_ID를 사용하여 통일
    from uuid import uuid4
    run_id = os.getenv("TRADER_RUN_ID")
    if not run_id:
        run_id = str(uuid4())
        os.environ["TRADER_RUN_ID"] = run_id
    
    # Create RunContext
    env = resolve_env(args.env)
    strategy = os.getenv("STRATEGY", "best_k_meta")
    gh_run_number = _parse_optional_int_env("GITHUB_RUN_ID") or _parse_optional_int_env("GITHUB_RUN_NUMBER")
    git_sha = os.getenv("GITHUB_SHA")
    ctx = RunContext.new(
        account_env=env,
        exec_mode=resolved_mode,
        strategy=strategy,
        gh_run_number=gh_run_number,
        git_sha=git_sha,
    )
    logger.info("[RUN_CONTEXT] run_id=%s env=%s strategy=%s gh_run_number=%s git_sha=%s", run_id, ctx.env, ctx.strategy, ctx.gh_run_number, ctx.git_sha)
    metrics: dict[str, int] = {}
    phase_for_log = "none"
    result_status = "UNKNOWN"
    main_exit_reason = "single_run"
    start_ts = time_mod.time()
    try:
        if smoke_enabled:
            run_once(args=args, engine=engine, ctx=ctx, loop_mode=False, window=None)
            return 0
        recovery_guard = _evaluate_session_recovery_guard(
            engine=engine,
            env=ctx.env,
            session_kind=_resolve_session_kind(),
            now=_get_now_kst(),
        )
        if recovery_guard.get("skip"):
            result_status = "SKIP_PHASE_WINDOW"
            phase_for_log = str(os.getenv("FORCE_PB1_PHASE") or "entry")
            main_exit_reason = str(recovery_guard.get("exit_reason") or "phase_guard_skip_late_schedule_no_recovery")
            session_kind = _resolve_session_kind(_get_now_kst())
            main_exit_reason = _normalize_phase_guard_exit_reason(session_kind, main_exit_reason)
            _log_main_run_summary(status=result_status, reason=main_exit_reason)
            return 0
        if recovery_guard.get("recovery_used"):
            main_exit_reason = str(recovery_guard.get("exit_reason") or "recovery_continue")
        _touched, _did_work, metrics, phase_for_log, result_status = run_once(
            args=args,
            engine=engine,
            ctx=ctx,
            loop_mode=False,
            window=None,
            max_seconds=max_seconds,
        )
        if _is_close_manual_replay_active():
            logger.info(
                "[TRADE_CLOSE][MANUAL_REPLAY][DONE] mode=%s status=%s phase=%s",
                _resolve_close_manual_mode(),
                result_status,
                phase_for_log,
            )
    except RuntimeError as exc:
        msg = str(exc)
        if msg.startswith("ENTRY_ABORT_PRECHECK:"):
            reason = msg.split(":", 1)[1]
            logger.error("[PB1][ENTRY][ABORT] reason=%s expected_as_of=%s db_rows=", reason, os.getenv("KR_EXPECTED_AS_OF") or os.getenv("AS_OF_OVERRIDE") or "")
            logger.info("[RUN_SUMMARY][RESULT] status=FAIL_PRECHECK reason=DB_EXACT_FINAL30_ZERO orders_intent=0 orders_ack=0")
            result_status = "FAIL_PRECHECK"
            main_exit_reason = "DB_EXACT_FINAL30_ZERO"
        else:
            logger.error(
                "[PB1][FATAL_GUARD] unexpected runtime error ctx_env=%s strategy_env=%s kis_env=%s",
                ctx.env,
                os.getenv("STRATEGY_ENV"),
                os.getenv("KIS_ENV"),
                exc_info=True,
            )
            result_status = "ERROR"
    except Exception:
        logger.error(
            "[PB1][FATAL_GUARD] unexpected error ctx_env=%s strategy_env=%s kis_env=%s",
            ctx.env,
            os.getenv("STRATEGY_ENV"),
            os.getenv("KIS_ENV"),
            exc_info=True,
        )
        result_status = "ERROR"
    finally:
        try:
            release_advisory_xact_lock(lock_conn, context=lock_context)
        except Exception as exc:
            logger.warning("[PB1][LOCK][RELEASE_FAIL] advisory lock release failed (ignoring): %s", exc)
        try:
            lock_conn.close()
        except Exception as exc:
            logger.warning("[PB1][LOCK][CLOSE_FAIL] lock connection close failed (ignoring): %s", exc)
        elapsed = time_mod.time() - start_ts
        logger.info(
            "[PB1][EXIT] reason=%s elapsed=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
            main_exit_reason,
            elapsed,
            max_seconds,
            "none",
            phase_for_log,
            metrics.get("balance_api_calls", 0),
            metrics.get("balance_cache_hits", 0),
            metrics.get("balance_tick_cache_hits", 0),
        )
        os.environ["PB1_LAST_RESULT_STATUS"] = str(result_status)
        os.environ["PB1_LAST_EXIT_REASON"] = str(main_exit_reason)
        normalized = normalize_session_result(
            status=result_status,
            reason=main_exit_reason,
            order_candidates=int(metrics.get("order_candidates", 0) or 0),
            api_submitted=int(metrics.get("api_submitted", 0) or metrics.get("submitted", 0) or 0),
            skipped=int(metrics.get("skipped", 0) or 0),
            skip_reasons=metrics.get("skip_reasons") or [],
        )
        _record_session_execution_marker(
            engine=engine,
            env=ctx.env,
            session_kind=_resolve_session_kind(),
            trade_date=_get_now_kst().date(),
            status=normalized.status,
            exit_reason=normalized.exit_reason,
            recovery_used=str(os.getenv("PB1_SESSION_RECOVERY_USED") or "0") == "1",
            completed=bool(normalized.completed),
            retryable=bool(normalized.retryable),
            sell_orders_ack=int(metrics.get("sell_orders_ack", metrics.get("sell_orders", 0)) or 0),
            entry_status="DONE" if normalized.completed else "ABORT",
            entry_abort_reason=None if normalized.completed else normalized.exit_reason,
            exit_code=0 if normalized.completed else 1,
        )
        _write_session_result_file({
            "status": result_status,
            "exit_reason": main_exit_reason,
            "sell_orders_ack": int(metrics.get("sell_orders_ack", metrics.get("sell_orders", 0)) or 0),
            "sell_orders": int(metrics.get("sell_orders", 0) or 0),
            "exit_evaluated_positions": int(metrics.get("exit_evaluated_positions", 0) or 0),
            "order_candidates": int(metrics.get("order_candidates", 0) or 0),
            "api_submitted": int(metrics.get("api_submitted", 0) or 0),
            "exit_submitted_codes": list(metrics.get("exit_submitted_codes", []) or []),
            "no_sellable_qty_terminal_codes": list(metrics.get("no_sellable_qty_terminal_codes", []) or []),
            "buy_orders": int(metrics.get("buy_orders", 0) or 0),
        })
    return _exit_code_for_status(result_status)


if __name__ == "__main__":
    raise SystemExit(main())
