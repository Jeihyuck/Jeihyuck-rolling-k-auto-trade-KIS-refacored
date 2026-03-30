from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import traceback
import time as time_mod
import copy
from collections import Counter
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

from trader.final30_quality import (
    format_final30_abort_message,
    normalize_final30_contract_row,
    summarize_entry_style_distribution,
    validate_trade_ready,
    verify_final30_scored_rows,
)
from zoneinfo import ZoneInfo

import pandas as pd

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
from trader.db.engine import make_engine
from trader.db.health import assert_db_ready
from trader.db.locks import acquire_advisory_lock, release_advisory_lock
from trader.db.migrate import run_migrations
from trader.db.repos import (
    FillsRepo,
    DerivedMinerviniRepo,
    LedgerEventsRepo,
    OrdersRepo,
    PositionsRepo,
    ReconcileLogRepo,
    RunsRepo,
    UniverseRepo,
)
from trader.diagnostics.nontrading_smoke import (
    nontrading_smoke_flag_path,
    run_nontrading_smoke_once,
    write_nontrading_smoke_flag,
)
from trader.kis_wrapper import KisAPI, KisBalanceUnavailable, KisTemporaryError
from trader.pb1_engine import PB1Engine, UniverseContext, resolve_pb1_phase
from trader.entry_engine import scan_all_strategies, calculate_position_size
from trader.reconcile_kis import reconcile_kis, reconcile_today
from trader.reconcile_db import close_stale_positions
from trader.run_context import RunContext
from trader.universe.build import build_universe
from trader.universe.mode import is_db_only_mode
from trader.time_utils import calc_market_window_kst, is_trading_weekday, now_kst, week_monday, is_market_open_kst, market_close_dt_kst, resolve_derived_as_of, resolve_trade_context, prev_business_day
from trader.utils.env import env_bool, parse_env_flag, resolve_mode, parse_bool_any
from trader.window_router import WindowDecision, decide_window
from trader.watchlist_builder import build_and_save_watchlist
from trader.db.repos import WatchlistRepo
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.indicators import compute_ma20_from_ohlcv, safe_nullable_float
from trader.strategies.pb1_minervini_v2 import MinerviniConfig

logger = logging.getLogger(__name__)
log = logger


def try_acquire_lock(*args, **kwargs):
    return acquire_advisory_lock(*args, **kwargs)


def is_trading_day(now: datetime) -> bool:
    return is_trading_weekday(now)

REQUIRED_SCORED_FINAL30_COLS = [
    "code",
    "name",
    "score_final",
    "tech_score",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "rs_percentile",
    "vcp_score",
    "entry_style_selected",
    "ma20",
    "ma50",
    "ma150",
    "atr_pct",
]
OPTIONAL_SCORING_ALTERNATIVE_COLS = [("close", "last_close")]
FLOW_OPTIONAL_COLS = [
    "investor_flow",
    "foreign_net_buy",
    "institutional_net_buy",
    "program_trade",
    "flow_score",
    "flow_rank",
    "flow_reason",
]

_FINAL30_LOAD_CACHE: dict[tuple[str, str], dict[str, Any]] = {}


def _missing_scored_cols(columns: list[str]) -> list[str]:
    cols = {str(c) for c in (columns or [])}
    missing = [c for c in REQUIRED_SCORED_FINAL30_COLS if c not in cols]
    for primary, alternative in OPTIONAL_SCORING_ALTERNATIVE_COLS:
        if primary in missing and alternative in cols:
            missing.remove(primary)
    return missing


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
            required_fields=REQUIRED_SCORED_FINAL30_COLS,
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
        required_fields=REQUIRED_SCORED_FINAL30_COLS,
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
        required_fields=REQUIRED_SCORED_FINAL30_COLS,
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


def load_trade_final30_scored(
    *,
    engine,
    env: str,
    as_of: str,
) -> dict[str, Any]:
    env_n = (env or "").strip().lower()
    as_of_s = str(as_of)
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
    flow_optional_missing = [col for col in FLOW_OPTIONAL_COLS if col not in contract_columns]
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
            logger.info("[TRADE][READY][OK] source=%s as_of=%s rows=%s", "db_pb1_watchlist_final_scored", as_of_date.isoformat(), len(df))
        except RuntimeError as exc:
            logger.error("[TRADE][PRECHECK][FINAL30] status=FAIL reason=db_contract_invalid")
            logger.error("[TRADE][FINAL30][FAIL] reason=missing_or_empty")
            logger.error("[TRADE][READY][FAIL] reason=final30_contract_invalid")
            logger.error("[TRADE][ABORT][FINAL30_INVALID][DETAIL] %s", format_final30_abort_message(trade_validate))
            logger.error("%s", exc)
            result = {
                "df": pd.DataFrame(),
                "source_name": "none",
                "as_of": as_of_s,
                "columns": [],
                "is_scored": False,
                "used_fallback": False,
                "file_mirror_present": any(file_mirror_stats.values()),
            }
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
        }
        _FINAL30_LOAD_CACHE[cache_key] = {**result, "df": df.copy(deep=True)}
        return result

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
    result = {
        "df": pd.DataFrame(),
        "source_name": "none",
        "as_of": as_of_s,
        "columns": [],
        "is_scored": False,
        "used_fallback": False,
        "file_mirror_present": any(file_mirror_stats.values()),
    }
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
        minervini_config_dict = {
            "rs_min": minervini_config.rs_min,
            "breakout_vol_mult_20": minervini_config.breakout_vol_mult_20,
            "heavy_vol_mult_10": minervini_config.heavy_vol_mult_10,
            "add_on_R": minervini_config.add_on_R,
            "max_pyramid_levels": minervini_config.max_pyramid_levels,
            "initial_stop_pct": minervini_config.initial_stop_pct,
            "time_stop_days": minervini_config.time_stop_days,
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
    mode_input = (os.getenv("MODE") or "").strip().lower()
    if mode_input == "trade" or os.getenv("PB1_TRADE_WATCHLIST_ONLY", "0") == "1":
        result = load_trade_final30_scored(engine=engine, env=env, as_of=as_of)
        df = result.get("df")
        if df is None or df.empty:
            raise RuntimeError("WATCHLIST_FINAL_NOT_FOUND")

        require_scored = os.getenv("TRADE_REQUIRE_PREP_FINAL30_SCORED", "1") == "1"
        if require_scored and not bool(result.get("is_scored")):
            raise RuntimeError("WATCHLIST_FINAL_SCORED_REQUIRED")

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
                "is_scored": bool(result.get("is_scored")),
                "columns": list(result.get("columns") or []),
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
    resolved_phase, phase_reason, phase_window = resolve_pb1_phase(now, trading_day, phase_seed)
    reasons: list[str] = []
    if not resolved_phase:
        fallback = (os.getenv("PB1_PHASE_DEFAULT") or "entry").strip().lower()
        resolved_phase = fallback if fallback else "entry"
        phase_reason = "default_env"
        reasons.append("phase_default_env")
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


def _normalize_window_phase(*, raw_window: Any, market_window: str, phase: str) -> tuple[str, str]:
    raw_name = ""
    raw_type = type(raw_window).__name__ if raw_window is not None else "NoneType"
    if isinstance(raw_window, str):
        raw_name = raw_window
    elif raw_window is not None:
        raw_name = str(getattr(raw_window, "name", "") or "")
    seed = (market_window or raw_name or "day").strip().lower()
    if seed in {"preopen", "morning", "intraday", "close"}:
        window_name = "intraday"
    elif seed == "after":
        window_name = "after"
    else:
        window_name = "day"
    phase_seed = (phase or "entry").strip().lower()
    if phase_seed in {"entry", "exit", "manage"}:
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
    logger.info("[BALANCE][CACHE] hit=%s", not force)


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


def run_once(
    *,
    args: argparse.Namespace,
    engine,
    ctx: RunContext,
    loop_mode: bool = False,
    window: WindowDecision | None = None,
    runtime_dir: Path | None = None,
    max_seconds: int = 0,
) -> tuple[list[Path], bool, dict[str, int], str, str]:
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
    intended_live = (os.getenv("STRATEGY_MODE") == "LIVE")
    
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
        if violations:
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
    }
    os.environ["AS_OF_OVERRIDE"] = as_of
    
    logger.info(
        "[ASOF][RUN_ONCE] trade_date=%s derived_as_of=%s reason=%s",
        trade_date.isoformat(),
        as_of,
        asof_reason,
    )
    logger.info("[ASOF][LOCK] trade_date=%s derived_as_of=%s immutable=1 source=db_contract", trade_date.isoformat(), as_of)
    logger.info("[ASOF][LOCK][VERIFY] trade_date=%s derived_as_of=%s run_ctx=%s", trade_date.isoformat(), as_of, run_ctx)
    
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
    if mode == "LIVE" and not intended_live:
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
    phase_seed = force_phase_env if force_phase_env else (None if args.phase == "auto" else args.phase)
    resolved_window, window_label, resolved_phase, phase_reason, phase_window, context_reasons = _resolve_market_context(
        now=now,
        trading_day=trading_day,
        market_window=market_window,
        window_override=args.window,
        phase_seed=phase_seed,
    )
    normalized_window_name, normalized_phase_name = _normalize_window_phase(
        raw_window=resolved_window,
        market_window=market_window,
        phase=resolved_phase,
    )
    run_ctx["window_name"] = normalized_window_name
    run_ctx["phase_name"] = normalized_phase_name
    logger.info("[ASOF][USE] component=runner value=%s source=run_ctx", run_ctx["derived_as_of"])
    
    # ✅ DIAG_FULL_EXEC: DIAG 모드에서 window/phase 강제 우회
    diag_full_exec = env_bool("PB1_DIAG_FULL_EXEC", False)
    if diag_full_exec and mode == "DIAG":
        # ✅ FORCE_PB1_PHASE가 설정되어 있으면 존중, 없으면 fallback to prep (for entry_scan)
        diag_phase = force_phase_env if force_phase_env else "prep"
        logger.info("[PB1][DIAG_FULL_EXEC] force window=day, phase=%s (bypass window/phase gates)", diag_phase)
        market_window = "day"
        window_label = "day"
        resolved_phase = diag_phase
        phase_reason = "diag_full_exec_override"
        context_reasons.append("diag_full_exec:forced_day_" + diag_phase)
    
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
    if action == "smoke" and mode == "DIAG" and diag_full:
        logger.info("[PB1][DIAG_FULL_EXEC] bypass smoke -> run engine once (no KIS HTTP)")
        action = "run"  # smoke 건너뛰고 엔진 실행
    elif action == "smoke" and not compute_only_full_run:
        _run_smoke(engine, kis_env=(os.getenv("KIS_ENV") or "practice").lower(), now=now)
        return [], False, {}, phase_for_log, "SMOKE"
    elif action == "smoke" and compute_only_full_run and ((run_ctx.get("window_name") == "after") or market_window == "after"):
        logger.info("[AFTER_COMPUTE_ONLY][ROUTE] reuse existing intraday trade pipeline")
        action = "run"

    # ✅ DIAG_FULL이면 윈도우 게이트 무시하고 계속 진행
    if not window and not close_cancel_only:
        if mode == "DIAG" and diag_full:
            # ✅ window 객체를 강제 생성하지 않고 문자열 컨텍스트로 고정
            phase_default = os.getenv("PB1_PHASE_DEFAULT", "entry")
            window = None
            window_label = "day"
            phase_for_log = phase_default
            run_ctx["window_name"] = "day"
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
        if args.phase == "auto" and not force_phase_env:
            phase_override_arg = "verify"
            phase_reason = "diagnostic"
        window = None
        window_label = "day"
        _apply_env_flags_if_needed(dry_run)

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
        "[PB1][TICK] now_kst=%s market_window=%s window=%s phase=%s reasons=%s",
        now.isoformat(),
        market_window,
        window_label,
        phase_for_log,
        context_reasons or ["none"],
    )

    def _remaining_seconds() -> float:
        if deadline_ts is None:
            return float("inf")
        return max(0.0, deadline_ts - time_mod.monotonic())

    remaining_s = _remaining_seconds()
    exit_short_circuit = phase_for_log == "exit" or window_label == "close"
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
            if kis:
                try:
                    ctx = RunContext(
                        run_id=run_id,
                        env=(os.getenv("KIS_ENV") or "practice").lower(),
                        strategy="pb1_pullback_close",
                        started_at=now,
                        dry_run=False,
                    )
                    reconcile_today(
                        engine=engine,
                        kis=kis,
                        ctx=ctx,
                    )
                    reconcile_ok = True
                except Exception:
                    logger.exception("[PB1][EXIT_SHORTCIRCUIT] reconcile_today failed")
            try:
                close_stale_positions(
                    engine=engine,
                    env=(os.getenv("KIS_ENV") or "practice").lower(),
                    strategy="pb1_pullback_close",
                    reason="exit_phase",
                    ts=now,
                )
                close_stale_ok = True
            except Exception:
                logger.exception("[PB1][EXIT_SHORTCIRCUIT] close_stale_positions failed")
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
            if kis:
                try:
                    ctx = RunContext(
                        run_id=run_id,
                        env=(os.getenv("KIS_ENV") or "practice").lower(),
                        strategy="pb1_pullback_close",
                        started_at=now,
                        dry_run=False,
                    )
                    reconcile_today(
                        engine=engine,
                        kis=kis,
                        ctx=ctx,
                    )
                    reconcile_ok = True
                except Exception:
                    logger.exception("[PB1][DEGRADED] reconcile_today failed")
            try:
                close_stale_positions(
                    engine=engine,
                    env=(os.getenv("KIS_ENV") or "practice").lower(),
                    strategy="pb1_pullback_close",
                    reason="budget_degraded",
                    ts=now,
                )
                close_stale_ok = True
            except Exception:
                logger.exception("[PB1][DEGRADED] close_stale_positions failed")
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
            universe_strategy = os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
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
            universe_strategy = os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
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
                precomputed_final30_df = pd.DataFrame(list(universe_ctx.members or []))
                if not precomputed_final30_df.empty:
                    final30_source_name = str((universe_ctx.meta or {}).get("source") or "db_pb1_watchlist_final_scored")
                    run_ctx["final30_source"] = final30_source_name
                    run_ctx["final30_locked"] = True
                    trade_use_precomputed_features = True
                    logger.info(
                        "[TRADE][FINAL30][DB_LOCK] env=%s as_of=%s rows=%s scored=%s immutable=1",
                        env_effective,
                        str(run_ctx.get("derived_as_of") or as_of),
                        len(precomputed_final30_df),
                        int(final30_source_name == "db_pb1_watchlist_final_scored"),
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
                    except Exception:
                        precomputed_universe_df = pd.DataFrame()
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
        kis: KisAPI | None = None
        allow_compute_without_kis = bool(
            compute_only_full_run
            or (not trading_day)
            or market_window == "after"
            or bool(compute_only_flags.get("force_block_live"))
        )
        try:
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
        if kis:
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
        if balance_state == BALANCE_STATE_UNKNOWN and not minervini_only:
            order_allowed = False
            entry_block_reason = entry_block_reason or "balance_unknown"
        elif PB1_REQUIRE_BALANCE_FOR_ENTRY and balance_state == BALANCE_STATE_STALE_OK and not minervini_only:
            order_allowed = False
            entry_block_reason = entry_block_reason or "balance_stale"
        if window_label not in {"preopen", "morning", "day"} and not minervini_only:
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
                if now.time() > cutoff_time:
                    force_compute_when_cutoff = (
                        env_bool("FORCE_COMPUTE_ON_CUTOFF", False)
                        or env_bool("BYPASS_ENTRY_CUTOFF_COMPUTE_ONLY", False)
                        or env_bool("FORCE_COMPUTE_WHEN_CUTOFF", False)
                        or env_bool("BYPASS_ENTRY_CUTOFF_FOR_COMPUTE", False)
                        or PB1_DIAG_IGNORE_ENTRY_CUTOFF
                    )
                    order_allowed = False
                    entry_block_reason = entry_block_reason or "entry_cutoff"
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

        logger.info(
            "[TRADE][PRECHECK][BALANCE] state=%s require_balance=%s allow_compute_without_kis=%s",
            balance_state,
            int(bool(PB1_REQUIRE_BALANCE_FOR_ENTRY)),
            int(bool(allow_compute_without_kis)),
        )
        if balance_state == BALANCE_STATE_UNKNOWN and PB1_REQUIRE_BALANCE_FOR_ENTRY and not allow_compute_without_kis:
            logger.error("[TRADE][READY][FAIL] reason=balance_precheck_failed")
            logger.warning("[PB1][DEGRADED] reason=balance_unknown -> skip trading")
            runs_repo.finish_run(run_record_id, status="DEGRADED", notes="balance_unknown")
            db_write_reasons.append("balance_degraded")
            _write_last_db_write(runtime_root_dir, run_id=str(run_record_id), reason="balance_degraded", now=now)
            return [], False, {}, phase_for_log, "DEGRADED_BALANCE_UNKNOWN"

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
        logger.info(
            "[TRADE][ENGINE_BOOT][PRECHECK_OK] final30_rows=%s balance_state=%s gate_order_allowed=%s phase=%s window=%s",
            int(len(precomputed_final30_df) if isinstance(precomputed_final30_df, pd.DataFrame) else 0),
            balance_state,
            int(bool(order_allowed)),
            phase_name_for_engine,
            window_name_for_engine,
        )
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
            result = engine_runner.run()
            if entry_scan_compat_failed:
                logger.info("[ENTRY_SCAN][FALLBACK] source=pb1_engine_internal_scan status=ok")

        if result is not None:
            result_reason = result.notes or "none"
            if not trading_day and (compute_only_full_run or dry_run or bool(compute_only_flags.get("force_block_live"))):
                result.status = "OK_NONTRADING_COMPUTE"
                result_reason = "WINDOW_BLOCKED_COMPUTE_ONLY"
            elif market_window == "after" and compute_only_full_run:
                result.status = "OK_WINDOW_BLOCKED_COMPUTE"
                result_reason = "WINDOW_BLOCKED_COMPUTE_ONLY"
            elif result.status in {"OK_NO_TRADE", "NO_TRADE"} and result.notes and "no_candidates" in result.notes:
                result.status = "OK_NO_CANDIDATES"
                result_reason = "NO_CANDIDATES_AFTER_RELAX"
            elif result.status in {"OK_NO_TRADE", "NO_TRADE"}:
                result.status = "OK_NO_TRADE"
                result_reason = result.notes or "NO_ORDER_INTENTS"
            logger.info("[RUN_SUMMARY][RESULT] status=%s reason=%s", result.status, result_reason)
            result.notes = result_reason
            if nontrading_eval_mode:
                _write_nontrading_eval_reports(
                    engine_runner=engine_runner,
                    result_status=result.status,
                )
        try:
            scanner_summary = getattr(engine_runner, "_scanner_summary", {}) or {}
            run_summary = getattr(engine_runner, "_run_summary_payload", {}) or {}
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
        logger.exception(
            "[PB1][FATAL_GUARD] unexpected error strategy_env=%s kis_env=%s derived_env=%s effective_env=%s",
            locals().get("env_strategy"),
            locals().get("env_kis"),
            locals().get("env_derived"),
            locals().get("env_effective"),
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
        if run_record_id:
            runs_repo.finish_run(run_record_id, status="FAILED", notes=str(exc))
            _write_last_db_write(runtime_root_dir, run_id=str(run_record_id), reason="failed", now=now)
        raise
    finally:
        pass
    metrics = {
        "balance_api_calls": result.balance_api_calls if result else 0,
        "balance_cache_hits": result.balance_cache_hits if result else 0,
        "balance_tick_cache_hits": result.balance_tick_cache_hits if result else 0,
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
    run_loop_minutes, loop_max_minutes, max_seconds, _ = _resolve_loop_limits()
    loop_mode = os.getenv("PB1_LOOP_MODE", "").upper()
    now = _get_now_kst()
    
    # ✅ UNTIL_CLOSE 모드: 장 마감까지 루프
    if loop_mode == "UNTIL_CLOSE":
        loop_deadline_dt = compute_loop_deadline(now)
        logger.info(
            "[PB1][LOOP] mode=UNTIL_CLOSE interval=%s deadline=%s",
            loop_interval,
            loop_deadline_dt.strftime("%Y-%m-%d %H:%M:%S"),
        )
    else:
        # 기존 로직 (max_minutes 기반)
        _, close_dt = _market_session(now)
        logger.info(
            "[PB1][LOOP] enabled interval=%s close=%s max_minutes=%s run_loop_minutes=%s",
            loop_interval,
            close_dt.isoformat(),
            loop_max_minutes,
            run_loop_minutes,
        )
    
    logger.info(
        "[PB1][LOOP] start now_kst=%s max_seconds=%s",
        now.isoformat(),
        max_seconds,
    )

    total_start_ts = time_mod.monotonic()
    engine = make_engine()
    lock_conn = engine.connect()
    if not try_acquire_lock(lock_conn):
        logger.warning("[PB1][LOOP] run lock unavailable -> exit")
        lock_conn.close()
        return
    exit_reason = "unknown"
    balance_api_calls = 0
    balance_cache_hits = 0
    balance_tick_cache_hits = 0
    last_phase = "none"
    loop_started_ts = total_start_ts
    loop_deadline = None
    loop_deadline_ts = None
    runtime_root_dir = runtime_root()
    
    # ✅ run_id SSOT: TRADER_RUN_ID를 사용하여 통일
    from uuid import uuid4
    run_id = os.getenv("TRADER_RUN_ID")
    if not run_id:
        run_id = str(uuid4())
        os.environ["TRADER_RUN_ID"] = run_id
    
    # Create RunContext for loop mode
    env = os.getenv("ENV", "live")
    strategy = os.getenv("STRATEGY", "best_k_meta")
    gh_run_number = _parse_optional_int_env("GITHUB_RUN_ID") or _parse_optional_int_env("GITHUB_RUN_NUMBER")
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
    
    try:
        _ensure_bootstrap_migrations(engine)
        _write_change_flag(False, ["init"])
        trade_start_ts = time_mod.monotonic()
        loop_started_ts = trade_start_ts
        loop_deadline_ts = trade_start_ts + max_seconds if max_seconds > 0 else None
        loop_deadline = None
        now_kst_value = _get_now_kst()
        if max_seconds > 0:
            loop_deadline = now_kst_value + timedelta(seconds=max_seconds)
        logger.info(
            "[PB1][CLOCK] total_start=%.3f trade_start=%.3f",
            total_start_ts,
            trade_start_ts,
        )
        logger.info(
            "[PB1][LOOP] deadline_ready now_kst=%s deadline=%s max_seconds=%s baseline=trade",
            now_kst_value.isoformat(),
            loop_deadline.isoformat() if loop_deadline else "none",
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
        while True:
            if stop_requested["value"]:
                exit_reason = "sigterm"
                break
            now = _get_now_kst()
            
            # ✅ UNTIL_CLOSE 모드: deadline 체크
            if loop_mode == "UNTIL_CLOSE":
                if now >= loop_deadline_dt:
                    logger.info("[PB1][LOOP] deadline reached -> exit now=%s deadline=%s",
                                now.strftime("%H:%M:%S"), loop_deadline_dt.strftime("%H:%M:%S"))
                    exit_reason = "loop_deadline"
                    break
            
            if _run_nontrading_smoke_if_needed(
                runtime_root_dir=runtime_root_dir,
                engine=engine,
                now=now,
                strategy_mode=strategy_mode,
            ):
                exit_reason = "nontrading_smoke_done"
                break
            
            # 기존 close_dt 체크 (UNTIL_CLOSE가 아닐 때만)
            if loop_mode != "UNTIL_CLOSE":
                _, close_dt = _market_session(now)
                if now >= close_dt:
                    logger.info("[PB1][LOOP] market closed -> exit")
                    exit_reason = "market_closed"
                    break
            
            elapsed_seconds = time_mod.monotonic() - loop_started_ts
            if max_seconds > 0 and elapsed_seconds >= max_seconds:
                logger.info("[PB1][LOOP] max_seconds=%s exiting", max_seconds)
                exit_reason = "loop_timeout"
                break
            if loop_max_minutes > 0:
                elapsed_min = (time_mod.monotonic() - loop_started_ts) / 60
                if elapsed_min >= loop_max_minutes:
                    logger.info("[PB1][LOOP] max minutes reached -> exit elapsed_min=%.1f", elapsed_min)
                    exit_reason = "loop_timeout"
                    break
            window = decide_window(now=now, override=args.window)
            if window is None:
                if max_seconds > 0 and os.getenv("PB1_LOOP_WAIT_OUTSIDE", "0") != "1":
                    next_start = _next_window_start(
                        now,
                        [
                            _parse_hhmm_to_time(MORNING_WINDOW_START),
                            _parse_hhmm_to_time(PB1_MORNING_WINDOW_END),
                            _parse_hhmm_to_time(PB1_ENTRY_WINDOW_END),
                        ],
                    )
                    logger.info(
                        "[PB1][LOOP] outside window -> exit (tick mode) next=%s",
                        next_start.isoformat() if next_start else "unknown",
                    )
                    exit_reason = "outside_window"
                    break

                next_start = _next_window_start(
                    now,
                    [
                        _parse_hhmm_to_time(MORNING_WINDOW_START),
                        _parse_hhmm_to_time(PB1_MORNING_WINDOW_END),
                        _parse_hhmm_to_time(PB1_ENTRY_WINDOW_END),
                    ],
                )
                if next_start:
                    sleep_for = (next_start - now).total_seconds()
                else:
                    sleep_for = loop_interval
                sleep_for = max(5.0, min(300.0, sleep_for))

                # FIX: RUN_LOOP_MINUTES(=max_seconds) 예산보다 더 오래 sleep 해야 하면
                # sleep 후 깨어나자마자 loop_timeout으로 끝나 "한 번 돌고 죽는" 것처럼 보인다.
                # 이 경우에는 sleep하지 않고 종료하여 다음 5분 tick(schedule)에 맡긴다.
                if max_seconds > 0:
                    remaining_budget = max_seconds - elapsed_seconds
                    if remaining_budget <= 0:
                        logger.info("[PB1][LOOP] budget exhausted before sleep -> exit")
                        exit_reason = "loop_timeout"
                        break
                    if sleep_for >= remaining_budget:
                        logger.info(
                            "[PB1][LOOP] outside window but insufficient budget -> exit "
                            "sleep=%.0fs remaining_budget=%.0fs next=%s",
                            sleep_for,
                            remaining_budget,
                            next_start.isoformat() if next_start else "unknown",
                        )
                        exit_reason = "outside_window_budget"
                        break
                logger.info(
                    "[PB1][LOOP] outside window -> sleep %.0fs next=%s",
                    sleep_for,
                    next_start.isoformat() if next_start else "unknown",
                )
                time_mod.sleep(sleep_for)
                continue

            if loop_deadline_ts and time_mod.monotonic() > loop_deadline_ts:
                logger.warning("[PB1][LOOP] deadline_exceeded_pre_trade deadline=%s", loop_deadline.isoformat())
                exit_reason = "deadline_exceeded_pre_trade"
                break
            remaining_budget_s = 0
            if loop_deadline_ts:
                remaining_budget_s = max(0, int(loop_deadline_ts - time_mod.monotonic()))
            try:
                _touched, _did_work, metrics, last_phase, result_status = run_once(
                    args=args,
                    engine=engine,
                    ctx=ctx,
                    loop_mode=True,
                    window=window,
                    max_seconds=remaining_budget_s,
                )
                balance_api_calls += metrics.get("balance_api_calls", 0)
                balance_cache_hits += metrics.get("balance_cache_hits", 0)
                balance_tick_cache_hits += metrics.get("balance_tick_cache_hits", 0)
                if result_status in {"NONTRADING_SMOKE_DONE", "NONTRADING_DAY_EXIT"}:
                    logger.info("[PB1][LOOP] non-trading-day exit status=%s", result_status)
                    exit_reason = result_status.lower()
                    break
                if result_status == "NO_TRADE":
                    logger.info("[PB1][LOOP] no trade -> exit")
                    exit_reason = "no_candidates"
                    break
            except Exception as exc:
                logger.error(
                    "[PB1][TICK][FATAL_GUARD] exception=%s\n%s",
                    exc,
                    traceback.format_exc(),
                )
                time_mod.sleep(3)
            time_mod.sleep(loop_interval)
        elapsed_trade = time_mod.monotonic() - loop_started_ts
        elapsed_total = time_mod.monotonic() - total_start_ts
        if exit_reason == "unknown":
            exit_reason = "shutdown"
        if max_seconds > 0 and elapsed_trade > max_seconds:
            logger.warning(
                "[PB1][EXIT][WARN] reason=%s elapsed_trade=%.1fs elapsed_total=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
                exit_reason,
                elapsed_trade,
                elapsed_total,
                max_seconds,
                loop_deadline.isoformat() if loop_deadline else "none",
                last_phase,
                balance_api_calls,
                balance_cache_hits,
                balance_tick_cache_hits,
            )
        else:
            logger.info(
                "[PB1][EXIT] reason=%s elapsed_trade=%.1fs elapsed_total=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
                exit_reason,
                elapsed_trade,
                elapsed_total,
                max_seconds,
                loop_deadline.isoformat() if loop_deadline else "none",
                last_phase,
                balance_api_calls,
                balance_cache_hits,
                balance_tick_cache_hits,
            )
    finally:
        try:
            release_advisory_lock(lock_conn)
        except Exception as exc:
            logger.warning("[PB1][LOCK][RELEASE_FAIL] advisory lock release failed (ignoring): %s", exc)
        try:
            lock_conn.close()
        except Exception as exc:
            logger.warning("[PB1][LOCK][CLOSE_FAIL] lock connection close failed (ignoring): %s", exc)


def _exit_code_for_status(status: str) -> int:
    if status in {"FAILED", "ERROR"}:
        return 1
    return 0


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
            result = load_trade_final30_scored(
                engine=watchlist_engine,
                env=watchlist_env,
                as_of=requested_as_of.isoformat(),
            )
            df = result.get("df")
            if df is None or df.empty:
                raise RuntimeError(
                    f"[TRADE][WATCHLIST_FINAL] missing_or_bad n=0 as_of_try={requested_as_of.isoformat()}"
                )
            require_scored = os.getenv("TRADE_REQUIRE_PREP_FINAL30_SCORED", "1") == "1"
            if require_scored and not bool(result.get("is_scored")):
                raise RuntimeError("[TRADE][WATCHLIST_FINAL] scored_required_but_missing")

            watchlist_loaded_for_guard = True
            rows = [dict(x) for x in df.to_dict(orient="records")]
            top10_codes = [str(item.get("code") or "").zfill(6) for item in rows[:10] if item.get("code")]
            logger.info(
                "[TRADE][WATCHLIST_FINAL][LOCK] env=%s strategy=%s requested_as_of=%s actual_as_of=%s n=%s",
                watchlist_env,
                result.get("source_name"),
                requested_as_of.isoformat(),
                str(result.get("as_of") or requested_as_of.isoformat()),
                len(rows),
            )
            logger.info("[TRADE][WATCHLIST_FINAL][TOP10] codes=%s", top10_codes)
            return datetime.strptime(str(result.get("as_of") or requested_as_of.isoformat()), "%Y-%m-%d").date()

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
        
        # PREP_DONE 체크는 derived_as_of 기준으로
        prep_done, prep_done_count = ledger_repo.prep_done_status(
            env=os.getenv("STRATEGY_ENV", "practice").lower(),
            as_of=derived_as_of,
        )
        logger.info(
            "[TRADE_TICK][PREP_DONE_CHECK] source=db_ledger prep_done=%s matched_events_count=%s derived_as_of=%s",
            int(prep_done),
            prep_done_count,
            derived_as_of.isoformat(),
        )
        if not prep_done:
            if allow_wl_only and watchlist_loaded_for_guard:
                logger.warning(
                    "[TRADE_TICK][PREP_BYPASS] prep_not_done but watchlist_final present -> continue derived_as_of=%s trade_date=%s",
                    derived_as_of.isoformat(),
                    trade_date.isoformat(),
                )
            else:
                logger.warning(
                    "[TRADE_TICK][SKIP] reason=PREP_NOT_DONE derived_as_of=%s trade_date=%s prep_done_check_source=db_ledger matched_events_count=%s",
                    derived_as_of.isoformat(),
                    trade_date.isoformat(),
                    prep_done_count,
                )
                return 0
        
        # DERIVED 체크도 derived_as_of 기준으로
        derived_repo = DerivedMinerviniRepo(engine)
        watchlist_repo = WatchlistRepo(engine)
        derived_env = (os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or derived_env or "practice").strip().lower()
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
            col for col in REQUIRED_SCORED_FINAL30_COLS
            if col not in [str(item) for item in (scored_contract.get("columns") or [])]
        ]
        scored_contract_repairable = _is_trade_repairable_db_contract(
            scored_contract,
            missing_critical_fields=scored_missing_critical,
        )
        db_contract_ok = bool(
            prep_done
            and derived_count > 0
            and watchlist_final_count == 30
            and watchlist_scored_count == 30
            and (scored_contract.get("ok") or scored_contract_repairable)
        )
        logger.info(
            "[TRADE_TICK][DB_CONTRACT] env=%s as_of=%s prep_done=%s derived_minervini=%s watchlist_final=%s watchlist_final_scored=%s repairable=%s db_contract_ok=%s",
            derived_env,
            derived_as_of.isoformat(),
            int(prep_done),
            derived_count,
            watchlist_final_count,
            watchlist_scored_count,
            int(scored_contract_repairable),
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
                "[TRADE_TICK][SKIP] reason=DB_CONTRACT_INCOMPLETE derived_as_of=%s trade_date=%s rows=%s uniq_codes=%s uniq_ranks=%s null_critical=%s",
                derived_as_of.isoformat(),
                trade_date.isoformat(),
                scored_contract.get("rows"),
                scored_contract.get("uniq_codes"),
                scored_contract.get("uniq_ranks"),
                scored_contract.get("null_critical"),
            )
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
                    return 0
            else:
                logger.warning(
                    "[TRADE_TICK][SKIP] reason=DERIVED_MISSING derived_as_of=%s trade_date=%s count=%d (fallback_disabled)",
                    derived_as_of.isoformat(),
                    trade_date.isoformat(),
                    derived_count,
                )
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
        final30_result = load_trade_final30_scored(
            engine=engine,
            env=derived_env,
            as_of=derived_as_of.isoformat(),
        )
        final30_df = final30_result.get("df")
        if final30_df is not None and not final30_df.empty:
            logger.info("[TRADE][PRECHECK][FINAL30] status=OK rows=%s", len(final30_df))
            logger.info(
                "[TRADE_TICK][FINAL30_SNAPSHOT][OK] derived_as_of=%s count=%d",
                derived_as_of.isoformat(),
                len(final30_df),
            )
            os.environ["PB1_SKIP_NEW_ENTRIES"] = "0"
        else:
            logger.error("[TRADE][PRECHECK][FINAL30] status=FAIL reason=missing_or_empty")
            logger.error("[ENTRY][BLOCK] missing scored final30 contract")
            logger.warning("[ENTRY][SKIP_NEW] reason=missing_scored_final30")
            os.environ["PB1_SKIP_NEW_ENTRIES"] = "1"
        
        watchlist_repo = WatchlistRepo(engine)
        watchlist_strategy = os.getenv("WATCHLIST_FINAL_STRATEGY_KEY", "pb1_watchlist_final").strip().lower()
        ttl_days = int(os.getenv("WATCHLIST_TTL_DAYS", "7"))
        max_back_days = int(os.getenv("WATCHLIST_MAX_BACK_DAYS", "3"))
        watchlist_env = resolve_env(args.env)

        watchlist_rows, used_as_of = watchlist_repo.load_watchlist(
            env=watchlist_env,
            strategy=watchlist_strategy,
            as_of=derived_as_of,
            allow_latest_fallback=True,
            ttl_days=ttl_days,
            max_back_days=max_back_days,
        )

        if not watchlist_rows or used_as_of is None:
            if os.getenv("PB1_SKIP_NEW_ENTRIES", "0") == "1":
                logger.warning("[ENTRY][SKIP_NEW] reason=missing_scored_final30")
                watchlist_rows = []
                used_as_of = derived_as_of
            else:
                raise RuntimeError("WATCHLIST_FINAL_NOT_FOUND")

        age_days = (derived_as_of - used_as_of).days
        if age_days > ttl_days:
            raise RuntimeError("WATCHLIST_FINAL_TTL_EXCEEDED")
        if watchlist_rows and len(watchlist_rows) != 30:
            raise RuntimeError(f"WATCHLIST_FINAL_SIZE_INVALID expected=30 actual={len(watchlist_rows)}")

        top10_codes = [str(item.get("code") or "").zfill(6) for item in watchlist_rows[:10] if item.get("code")]
        logger.info(
            "[TRADE][WATCHLIST_FINAL][LOCK] env=%s strategy=%s requested_as_of=%s actual_as_of=%s n=%s",
            watchlist_env,
            watchlist_strategy,
            derived_as_of.isoformat(),
            used_as_of.isoformat(),
            len(watchlist_rows),
        )
        logger.info("[TRADE][WATCHLIST_FINAL][TOP10] codes=%s", top10_codes)

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
    lock_conn = engine.connect()
    if not try_acquire_lock(lock_conn):
        logger.warning("[PB1][RUN] run lock unavailable -> exit")
        lock_conn.close()
        return 0
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
    start_ts = time_mod.time()
    try:
        if smoke_enabled:
            run_once(args=args, engine=engine, ctx=ctx, loop_mode=False, window=None)
            return 0
        _touched, _did_work, metrics, phase_for_log, result_status = run_once(
            args=args,
            engine=engine,
            ctx=ctx,
            loop_mode=False,
            window=None,
            max_seconds=max_seconds,
        )
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
            release_advisory_lock(lock_conn)
        except Exception as exc:
            logger.warning("[PB1][LOCK][RELEASE_FAIL] advisory lock release failed (ignoring): %s", exc)
        try:
            lock_conn.close()
        except Exception as exc:
            logger.warning("[PB1][LOCK][CLOSE_FAIL] lock connection close failed (ignoring): %s", exc)
        elapsed = time_mod.time() - start_ts
        logger.info(
            "[PB1][EXIT] reason=single_run elapsed=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
            elapsed,
            max_seconds,
            "none",
            phase_for_log,
            metrics.get("balance_api_calls", 0),
            metrics.get("balance_cache_hits", 0),
            metrics.get("balance_tick_cache_hits", 0),
        )
    return _exit_code_for_status(result_status)


if __name__ == "__main__":
    raise SystemExit(main())
