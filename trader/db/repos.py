from __future__ import annotations

from datetime import date, datetime, timedelta
import logging, os
import time
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID, uuid4

import pandas as pd
import pytz
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, StatementError, IntegrityError, DBAPIError, ProgrammingError, TimeoutError as SATimeoutError
from sqlalchemy import Engine, and_, func, or_, select, bindparam
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .schema import (
    FILLS,
    LEDGER_EVENTS,
    ORDERS,
    POSITIONS,
    PRICE_DAILY,
    RUNS,
    UNIVERSE_MEMBERS,
    UNIVERSE_CURRENT,
    UNIVERSE_RUNS,
    PB1_WATCHLIST,
    SchemaTables,
    schema_for_engine,
    uuid_value_for_url,
)
from trader.db.json_safe import json_sanitize
from trader.db.retry import run_with_db_retry
from trader.constants import (
    CRITICAL_SCORED_COLS,
    FINAL30_SCORED_IDENTITY_COLS,
    FINAL30_SCORED_PERSIST_COLS,
    FLOW_OPTIONAL_COLS,
    OPTIONAL_FLOW_COLS,
    REQUIRED_FINAL30_SCORED_COLS,
)
from trader.final30_quality import normalize_final30_contract_row, summarize_final30_quality, verify_final30_scored_rows
from trader.indicators import safe_nullable_float
from trader.time_utils import now_kst
from trader.time_coerce import to_date
from trader.run_context import RunContext
from trader.utils.ids import assert_uuid

logger = logging.getLogger(__name__)


def _normalize_run_id_text(value: Any | None) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None

__all__ = [
    "RunsRepo",
    "UniverseRepo",
    "OrdersRepo",
    "FillsRepo",
    "PositionsRepo",
    "PracticeAccountResetRepo",
    "LedgerEventsRepo",
    "ReconcileLogRepo",
    "PositionRepo",  # Backward compatibility
    "WatchlistRepo",
    "DerivedMinerviniRepo",
    "DerivedFlowRepo",
    "WatchlistSnapshotRepo",
    "MinerviniSnapshotRepo",
    "EntryDecisionRepo",
    "ExitAnalysisRepo",
    "save_watchlist",
    "load_watchlist",
    "save_pb1_watchlist_rows",
    "load_pb1_watchlist_codes",
    "load_watchlist_scored",
    "load_final30_scored_exact",
    "load_final30_scored_db_only",
    "FLOW_OPTIONAL_COLS",
    "REQUIRED_FINAL30_SCORED_COLS",
    "FINAL30_SCORED_PERSIST_COLS",
    "CRITICAL_SCORED_COLS",
    "OPTIONAL_FLOW_COLS",
    "FINAL30_SCORED_DB_CONTRACT_FIELDS",
    "FINAL30_SCORED_REQUIRED_ROWS",
    "ScoredWatchlistError",
    "ScoredWatchlistNotFoundError",
    "ScoredWatchlistInvalidError",
    "summarize_final30_scored_contract",
    "verify_final30_scored_contract",
]

FINAL30_SCORED_SAVE_REQUIRED_FIELDS = [
    "code",
    "as_of",
    "rank",
    "score",
    "score_final",
    "tech_score",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "entry_style_selected",
    "close",
    "ma20",
    "ma50",
    "ma150",
    "atr_pct",
    "rs_percentile",
    "vcp_score",
]

FINAL30_NUMERIC_RESTORE_ALIASES: dict[str, tuple[str, ...]] = {
    "close": ("close", "last_close", "close_price"),
    "ma20": ("ma20", "ma_20", "sma20", "ma20_price"),
    "ma50": ("ma50", "ma_50", "sma50", "ma50_price"),
    "ma150": ("ma150", "ma_150", "sma150", "ma150_price"),
    "atr_pct": ("atr_pct", "atr", "atrp", "atr_percent"),
    "rs_percentile": ("rs_percentile", "rs_pctile", "rs_score"),
    "breakout_score": ("breakout_score", "score_breakout"),
    "pullback_score": ("pullback_score", "score_pullback"),
    "momentum_score": ("momentum_score", "score_momentum"),
    "tech_score": ("tech_score", "score_tech"),
    "score_final": ("score_final", "final_score", "score"),
    "vcp_score": ("vcp_score",),
}

FINAL30_SCORED_DB_CONTRACT_FIELDS = [
    "code",
    "as_of",
    "rank_final30",
    "score_final",
    "tech_score",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "entry_style_selected",
    "ma20",
    "ma50",
    "ma150",
    "rs_percentile",
    "vcp_score",
    "atr_pct",
    "close",
    "reasons",
    "filters_passed",
    "filters_failed",
]

FINAL30_SCORED_REQUIRED_ROWS = 30
FINAL30_SCORED_NULLABLE_FIELDS = {
    "close",
    "ma20",
    "ma50",
    "ma150",
    "atr_pct",
    "rs_percentile",
}
FINAL30_SCORED_ZERO_INVALID_FIELDS = {"ma20", "ma50", "ma150"}

SCORED_WATCHLIST_STRATEGIES = {
    "pb1_watchlist_final_scored",
}
INTERMEDIATE_SCORED_STRATEGIES = {
    "pb1_universe_scored",
    "pb1_pool120",
    "pb1_top50",
}

ENTRY_META_SCALAR_KEYS = (
    "entry_reason",
    "entry_style_selected",
    "entry_decision_family",
    "entry_rule_version",
)


class ScoredWatchlistError(RuntimeError):
    def __init__(
        self,
        reason: str,
        *,
        expected_strategy: str,
        actual_strategy: str,
        rows: int,
        missing_cols: Iterable[str] | None = None,
    ) -> None:
        self.reason = str(reason)
        self.expected_strategy = str(expected_strategy)
        self.actual_strategy = str(actual_strategy)
        self.rows = int(rows)
        self.missing_cols = [str(col) for col in (missing_cols or [])]
        super().__init__(
            f"{self.reason}: expected_strategy={self.expected_strategy} actual_strategy={self.actual_strategy} "
            f"rows={self.rows} missing_cols={self.missing_cols}"
        )


class ScoredWatchlistNotFoundError(ScoredWatchlistError):
    pass


class ScoredWatchlistInvalidError(ScoredWatchlistError):
    pass


def _merge_json_dict(base: Any, incoming: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if isinstance(base, dict):
        merged.update(base)
    if isinstance(incoming, dict):
        merged.update(incoming)
    return json_sanitize(merged)


def _safe_float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _restore_numeric_from_sources(*sources: Any, aliases: tuple[str, ...]) -> float | None:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for alias in aliases:
            if alias not in source:
                continue
            numeric = safe_nullable_float(source.get(alias))
            if numeric is not None:
                return float(numeric)
    return None


def _field_null_counts(rows: List[Dict[str, Any]], fields: Iterable[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for field in fields:
        counts[str(field)] = sum(1 for row in (rows or []) if _contract_value_missing((row or {}).get(str(field))))
    return counts


def _normalize_final30_score_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    canonical = safe_nullable_float(row.get("score_final"))
    if canonical is None:
        canonical = safe_nullable_float(row.get("final_score"))
    if canonical is None:
        canonical = safe_nullable_float(row.get("score"))
    canonical_value = float(canonical) if canonical is not None else None
    row["score"] = canonical_value
    row["score_final"] = canonical_value
    row["final_score"] = canonical_value
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    meta["score"] = canonical_value
    meta["score_final"] = canonical_value
    meta["final_score"] = canonical_value
    row["meta"] = meta
    return row


def _roundtrip_value_matches(lhs: Any, rhs: Any) -> bool:
    lhs_num = safe_nullable_float(lhs)
    rhs_num = safe_nullable_float(rhs)
    if lhs_num is not None or rhs_num is not None:
        if lhs_num is None or rhs_num is None:
            return False
        return abs(float(lhs_num) - float(rhs_num)) < 1e-9
    if _contract_value_missing(lhs) and _contract_value_missing(rhs):
        return True
    return lhs == rhs


def _roundtrip_mismatch_counts(
    source_rows: List[Dict[str, Any]],
    loaded_rows: List[Dict[str, Any]],
    *,
    fields: Iterable[str],
) -> Dict[str, int]:
    source_by_code = {
        str((row or {}).get("code") or "").zfill(6): normalize_final30_contract_row(row)
        for row in (source_rows or [])
        if (row or {}).get("code")
    }
    loaded_by_code = {
        str((row or {}).get("code") or "").zfill(6): normalize_final30_contract_row(row)
        for row in (loaded_rows or [])
        if (row or {}).get("code")
    }
    codes = sorted(set(source_by_code) | set(loaded_by_code))
    mismatch_counts: Dict[str, int] = {}
    for field in fields:
        mismatch_counts[str(field)] = sum(
            1
            for code in codes
            if not _roundtrip_value_matches(
                (source_by_code.get(code) or {}).get(str(field)),
                (loaded_by_code.get(code) or {}).get(str(field)),
            )
        )
    return mismatch_counts


def _log_scored_sample(prefix: str, rows: List[Dict[str, Any]], *, limit: int = 5) -> None:
    for row in (rows or [])[:limit]:
        normalized = normalize_final30_contract_row(row)
        logger.info(
            "%s code=%s close=%s ma20=%s ma50=%s ma150=%s atr_pct=%s rs_percentile=%s breakout_score=%s pullback_score=%s momentum_score=%s score_final=%s",
            prefix,
            normalized.get("code", ""),
            normalized.get("close"),
            normalized.get("ma20"),
            normalized.get("ma50"),
            normalized.get("ma150"),
            normalized.get("atr_pct"),
            normalized.get("rs_percentile"),
            normalized.get("breakout_score"),
            normalized.get("pullback_score"),
            normalized.get("momentum_score"),
            normalized.get("score_final"),
        )


def _entry_meta_columns(entry_meta: dict[str, Any] | None, *, json_field: str) -> dict[str, Any]:
    payload = json_sanitize(entry_meta or {})
    return {
        "entry_reason": payload.get("entry_reason"),
        "entry_style_selected": payload.get("entry_style_selected"),
        "entry_decision_family": payload.get("entry_decision_family"),
        json_field: payload,
        "stop_price_at_entry": _safe_float_or_none(payload.get("stop_price_at_entry")),
        "pivot_price_at_entry": _safe_float_or_none(payload.get("pivot_price_at_entry")),
        "entry_rule_version": payload.get("entry_rule_version"),
    }


def _coerce_uuid(value: Any, *, uses_native_uuid: bool, database_url: str) -> Any:
    return uuid_value_for_url(database_url, value if isinstance(value, UUID) else value)


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _norm_env(env: str | None) -> str:
    return (env or "").strip().lower()


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _order_lookup_fail_open_default() -> bool:
    explicit = os.getenv("PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT")
    if explicit is None:
        return _practice_env_default_fail_open()
    return _env_flag("PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT", default=True)


def _practice_env_default_fail_open() -> bool:
    strategy_env = _norm_env(os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice")
    return strategy_env == "practice"


def _lookup_fail_open_default() -> bool:
    if _practice_env_default_fail_open():
        return True
    if _norm_strategy(os.getenv("STRATEGY_MODE")) == "diag":
        return True
    if _env_flag("NO_TRADE", default=False):
        return True
    if str(os.getenv("MANUAL_MODE") or "").strip():
        return True
    if _norm_env(os.getenv("GITHUB_EVENT_NAME")) == "workflow_dispatch":
        return True
    workflow_name = str(os.getenv("GITHUB_WORKFLOW") or "").strip().lower()
    if "manual" in workflow_name or "dispatch" in workflow_name:
        return True
    return False


def _resolve_lookup_fail_open() -> bool:
    explicit = os.getenv("PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT")
    if explicit is not None:
        return _env_flag(
            "PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT",
            default=_lookup_fail_open_default(),
        )
    return _lookup_fail_open_default()


def _norm_strategy(strategy: str | None) -> str:
    return (strategy or "").strip().lower()


def _contract_value_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _extract_rank_candidates(rows: List[Dict[str, Any]], field: str) -> list[int]:
    values: list[int] = []
    for row in rows:
        value = (row or {}).get(field)
        if _contract_value_missing(value):
            continue
        try:
            values.append(int(value))
        except Exception:
            continue
    return values


def _select_contract_rank_values(rows: List[Dict[str, Any]]) -> tuple[list[int], str]:
    for field in ("rank_final30", "rank"):
        values = _extract_rank_candidates(rows, field)
        if values and len(set(values)) > 1:
            return values, field
    return list(range(len(rows))), "synthetic_for_diag"


def summarize_final30_scored_contract(
    rows: List[Dict[str, Any]],
    *,
    env: str,
    as_of: date | str,
    expected_rows: int = FINAL30_SCORED_REQUIRED_ROWS,
) -> Dict[str, Any]:
    expected_as_of = to_date(as_of).isoformat()
    normalized_rows = [normalize_final30_contract_row(row) for row in (rows or []) if isinstance(row, dict)]
    all_columns = sorted({str(key) for row in normalized_rows for key in row.keys()})
    missing_fields = [field for field in FINAL30_SCORED_DB_CONTRACT_FIELDS if field not in all_columns]

    codes = [str((row or {}).get("code") or "").zfill(6) for row in normalized_rows if str((row or {}).get("code") or "").strip()]
    uniq_codes = len(set(codes))

    rank_values, rank_source = _select_contract_rank_values(normalized_rows)
    uniq_ranks = len(set(rank_values))
    rank_warn = rank_source == "synthetic_for_diag" or uniq_ranks != int(expected_rows)

    as_of_values = sorted({str((row or {}).get("as_of") or "")[:10] for row in normalized_rows if str((row or {}).get("as_of") or "").strip()})
    null_critical = sum(
        1
        for row in normalized_rows
        for field in FINAL30_SCORED_DB_CONTRACT_FIELDS
        if field not in FINAL30_SCORED_NULLABLE_FIELDS and _contract_value_missing((row or {}).get(field))
    )
    null_warnings = sum(
        1
        for row in normalized_rows
        for field in FINAL30_SCORED_NULLABLE_FIELDS
        if _contract_value_missing((row or {}).get(field))
    )
    critical_numeric_zero_invalid = sum(
        1
        for row in normalized_rows
        for field in FINAL30_SCORED_ZERO_INVALID_FIELDS
        if not _contract_value_missing((row or {}).get(field)) and float((row or {}).get(field) or 0.0) == 0.0
    )
    atr_pct_zero_invalid_when_close_positive = sum(
        1
        for row in normalized_rows
        if not _contract_value_missing((row or {}).get("atr_pct"))
        and float((row or {}).get("atr_pct") or 0.0) == 0.0
        and not _contract_value_missing((row or {}).get("close"))
        and float((row or {}).get("close") or 0.0) > 0.0
    )
    verifier = verify_final30_scored_rows(
        normalized_rows,
        required_rows=int(expected_rows),
        required_fields=FINAL30_SCORED_DB_CONTRACT_FIELDS,
        source="db",
    )
    quality = summarize_final30_quality(pd.DataFrame(normalized_rows), required_rows=int(expected_rows))
    quality_failures: list[str] = list(verifier.get("errors", []))
    quality_soft_failures: list[str] = list(verifier.get("warnings", []))

    ok = (
        bool(verifier.get("ok"))
        and null_critical == 0
        and critical_numeric_zero_invalid == 0
        and atr_pct_zero_invalid_when_close_positive == 0
    )
    return {
        **verifier,
        "rows": len(normalized_rows),
        "uniq_codes": uniq_codes,
        "uniq_ranks": uniq_ranks,
        "rank_source": rank_source,
        "rank_warn": bool(rank_warn),
        "null_critical": null_critical,
        "null_warnings": null_warnings,
        "critical_numeric_zero_invalid": sorted(FINAL30_SCORED_ZERO_INVALID_FIELDS),
        "critical_numeric_zero_invalid_count": critical_numeric_zero_invalid,
        "atr_pct_zero_invalid_when_close_positive": atr_pct_zero_invalid_when_close_positive,
        "as_of_values": as_of_values,
        "missing_fields": missing_fields,
        "env": _norm_env(env),
        "as_of": expected_as_of,
        "expected_rows": int(expected_rows),
        "quality": quality,
        "quality_failures": quality_failures,
        "quality_soft_failures": quality_soft_failures,
        "ok": bool(ok),
        "rows_data": normalized_rows,
        "columns": all_columns,
    }


def save_pb1_watchlist_rows(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date,
    rows: List[Dict[str, Any]],
) -> None:
    strategy_n = _norm_strategy(strategy)
    if strategy_n == "pb1_watchlist_final_scored":
        _save_pb1_final30_rows_scored_strict(
            engine,
            env=env,
            strategy=strategy,
            as_of=as_of,
            rows=rows,
        )
        return
    
    if strategy_n in INTERMEDIATE_SCORED_STRATEGIES:
        _save_pb1_scored_stage_rows_relaxed(
            engine,
            env=env,
            strategy=strategy,
            as_of=as_of,
            rows=rows,
        )
        return

    _save_pb1_watchlist_rows_plain(
        engine,
        env=env,
        strategy=strategy,
        as_of=as_of,
        rows=rows,
    )


def _save_pb1_watchlist_rows_plain(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date,
    rows: List[Dict[str, Any]],
) -> None:
    env_n = _norm_env(env)
    strategy_n = _norm_strategy(strategy)
    as_of_date = to_date(as_of)

    if not rows:
        logger.warning("[WATCHLIST][SAVE] empty rows -> skip env=%s strategy=%s as_of=%s", env_n, strategy_n, as_of_date)
        return

    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(
            sa.delete(schema.pb1_watchlist).where(
                and_(
                    schema.pb1_watchlist.c.env == env_n,
                    schema.pb1_watchlist.c.strategy == strategy_n,
                    schema.pb1_watchlist.c.as_of == as_of_date,
                )
            )
        )

        payload = [
            {
                "env": env_n,
                "strategy": strategy_n,
                "as_of": as_of_date,
                "code": str(r.get("code") or "").zfill(6),
                "rank": int(r.get("rank", 0)),
                "score": float(r["score"]) if r.get("score") is not None else None,
                "meta": json_sanitize(r.get("meta") or {}),
            }
            for r in rows
            if r.get("code")
        ]
        if payload:
            conn.execute(sa.insert(schema.pb1_watchlist), payload)

    logger.info(
        "[WATCHLIST][SAVE] env=%s strategy=%s as_of=%s members=%s",
        env_n,
        strategy_n,
        as_of_date,
        len(payload),
    )


def _save_pb1_scored_stage_rows_relaxed(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date,
    rows: List[Dict[str, Any]],
) -> None:
    """Save intermediate stage (universe/pool/top50) with relaxed validation."""
    env_n = _norm_env(env)
    strategy_n = _norm_strategy(strategy)
    as_of_date = to_date(as_of)

    if not rows:
        logger.info(
            "[STAGE_SAVE][RELAXED] strategy=%s rows=0 skip",
            strategy_n,
        )
        return

    logger.info(
        "[STAGE_SAVE][RELAXED] strategy=%s rows=%d",
        strategy_n,
        len(rows),
    )

    # Validate basic contract
    if not all((row or {}).get("code") for row in rows):
        logger.warning(
            "[STAGE_SAVE][RELAXED][WARN] strategy=%s has_code_missing=%d",
            strategy_n,
            sum(1 for row in rows if not (row or {}).get("code")),
        )

    # Build payload - permissive approach
    payload: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        if not (row or {}).get("code"):
            continue
        
        # Use basic scoring fields if available
        score_val = None
        for field in ["score_final", "final_score", "score_tech", "tech_score", "score"]:
            candidate = (row or {}).get(field)
            if candidate is not None:
                try:
                    score_val = float(candidate)
                    break
                except Exception:
                    pass
        
        entry = {
            "env": env_n,
            "strategy": strategy_n,
            "as_of": as_of_date,
            "code": str((row or {}).get("code") or "").zfill(6),
            "rank": int((row or {}).get("rank", idx)),
            "score": score_val,
            "meta": json_sanitize(normalize_final30_contract_row(row or {})),
        }
        payload.append(entry)

    if not payload:
        logger.warning(
            "[STAGE_SAVE][RELAXED][NO_VALID] strategy=%s",
            strategy_n,
        )
        return

    # Save to DB
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(
            sa.delete(schema.pb1_watchlist).where(
                and_(
                    schema.pb1_watchlist.c.env == env_n,
                    schema.pb1_watchlist.c.strategy == strategy_n,
                    schema.pb1_watchlist.c.as_of == as_of_date,
                )
            )
        )
        conn.execute(sa.insert(schema.pb1_watchlist), payload)

    logger.info(
        "[WATCHLIST][SAVE] env=%s strategy=%s as_of=%s members=%s",
        env_n,
        strategy_n,
        as_of_date,
        len(payload),
    )

    # Verify row count only
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        count_stmt = (
            select(func.count())
            .select_from(schema.pb1_watchlist)
            .where(
                and_(
                    schema.pb1_watchlist.c.env == env_n,
                    schema.pb1_watchlist.c.strategy == strategy_n,
                    schema.pb1_watchlist.c.as_of == as_of_date,
                )
            )
        )
        saved_count = int(conn.execute(count_stmt).scalar() or 0)
        if saved_count != len(payload):
            logger.warning(
                "[STAGE_SAVE][RELAXED][VERIFY_FAIL] strategy=%s expected=%d actual=%d",
                strategy_n,
                len(payload),
                saved_count,
            )
        else:
            logger.info(
                "[STAGE_SAVE][RELAXED][VERIFY_OK] strategy=%s rows=%d",
                strategy_n,
                saved_count,
            )


def _build_scored_payload_row(row: Dict[str, Any], *, as_of_date: date, idx: int) -> Dict[str, Any]:
    src = _normalize_final30_score_fields(normalize_final30_contract_row(row))
    sf = safe_nullable_float(src.get("score_final"))
    if sf is None:
        raise ValueError(f"MISSING_SCORE_FINAL code={src.get('code')}")

    src["score"] = float(sf)
    src["score_final"] = float(sf)
    src["final_score"] = float(sf)

    meta = src.get("meta") if isinstance(src.get("meta"), dict) else {}
    meta["score"] = float(sf)
    meta["score_final"] = float(sf)
    meta["final_score"] = float(sf)
    src["meta"] = meta

    if src.get("score_liq") is not None and float(src["score_liq"]) > 1000:
        logger.warning(
            "[SANITY][SCORE_LIQ_ABNORMAL] code=%s score_liq=%s",
            src.get("code"),
            src.get("score_liq"),
        )

    meta_score = (src.get("meta") or {}).get("score")
    if meta_score is not None and abs(float(meta_score) - float(src["score_final"])) > 1e-9:
        raise ValueError(
            f"META_SCORE_CORRUPTED code={src.get('code')} meta_score={meta_score} score_final={src['score_final']}"
        )

    code = str(src.get("code") or "").zfill(6)
    if not code:
        return {}

    rank_val = src.get("rank")
    if rank_val is None:
        rank_val = src.get("rank_final30")
    try:
        rank = int(rank_val if rank_val is not None else idx)
    except Exception:
        rank = idx

    # Trade AM failures here were caused by persistence schema mismatch, not by flow or scoring.
    # Keep the scored contract flattened before nesting it into meta so DB round-trip restores the same row schema.
    payload_body = {
        "as_of": as_of_date.isoformat(),
        "code": code,
        "name": src.get("name"),
        "rank": rank,
        "rank_pool120": src.get("rank_pool120"),
        "rank_top50": src.get("rank_top50"),
        "rank_final30": src.get("rank_final30") if src.get("rank_final30") is not None else rank,
        "score": src.get("score_final"),
        "score_final": src.get("score_final"),
        "final_score": src.get("score_final"),
        "score_flow": src.get("score_flow"),
        "score_liq": src.get("score_liq"),
        "score_tech": src.get("score_tech"),
        "tech_score": src.get("tech_score"),
        "flow_score": src.get("flow_score"),
        "breakout_score": src.get("breakout_score"),
        "pullback_score": src.get("pullback_score"),
        "momentum_score": src.get("momentum_score"),
        "entry_style_selected": src.get("entry_style_selected"),
        "entry_component": src.get("entry_component"),
        "rs_pctile": src.get("rs_percentile"),
        "rs_percentile": src.get("rs_percentile"),
        "rs_score": src.get("rs_score"),
        "vcp_score": src.get("vcp_score"),
        "trend_score": src.get("trend_score"),
        "atr_pct": src.get("atr_pct"),
        "pullback_pct": src.get("pullback_pct"),
        "foreign_20_ratio": src.get("foreign_20_ratio"),
        "inst_20_ratio": src.get("inst_20_ratio"),
        "liq_avg": src.get("liq_avg"),
        "last_close": src.get("last_close") if src.get("last_close") is not None else src.get("close"),
        "close": src.get("close"),
        "volume": src.get("volume"),
        "volume_avg20": src.get("volume_avg20"),
        "ma20": src.get("ma20"),
        "ma50": src.get("ma50"),
        "ma150": src.get("ma150"),
        "rows": src.get("rows"),
        "scores": src.get("scores") if isinstance(src.get("scores"), dict) else {},
        "reasons": src.get("reasons"),
        "reject_reasons": src.get("reject_reasons"),
        "filters_passed": src.get("filters_passed"),
        "filters_failed": src.get("filters_failed"),
    }
    payload_with_meta = {
        **payload_body,
        "meta": _merge_json_dict(src.get("meta"), payload_body),
    }
    payload_meta = json_sanitize(normalize_final30_contract_row(payload_with_meta))
    payload_meta = _normalize_final30_score_fields(payload_meta)
    return {
        "env": None,
        "strategy": None,
        "as_of": as_of_date,
        "code": code,
        "rank": rank,
        "score": payload_meta.get("score_final"),
        "meta": payload_meta,
    }


def _save_pb1_final30_rows_scored_strict(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date,
    rows: List[Dict[str, Any]],
) -> None:
    """Save final30 with strict contract validation."""
    env_n = _norm_env(env)
    strategy_n = _norm_strategy(strategy)
    as_of_date = to_date(as_of)

    # Developer guard: only final30 strategy should call this
    if strategy_n != "pb1_watchlist_final_scored":
        raise ValueError(
            f"DEVELOPER_ERROR_STRICT_FINAL30_SAVER_CALLED_FOR_NONFINAL_STAGE strategy={strategy_n} rows={len(rows)}"
        )
    
    # final30 must have exactly 30 rows
    if len(rows) != 30:
        raise ValueError(
            f"FINAL30_SCORED_CONTRACT_ROWS_MISMATCH strategy={strategy_n} expected=30 actual={len(rows)}"
        )

    if not rows:
        logger.warning("[WATCHLIST][SAVE_SCORED] empty rows -> skip env=%s strategy=%s as_of=%s", env_n, strategy_n, as_of_date)
        return

    raw_keys = set()
    for row in rows:
        raw_keys.update((row or {}).keys())
    missing_critical_from_source = [col for col in CRITICAL_SCORED_COLS if col not in raw_keys]
    if missing_critical_from_source:
        raise ValueError(
            f"[WATCHLIST][SAVE_SCORED][FAIL] missing_critical_cols={missing_critical_from_source} strategy={strategy_n}"
        )

    source_rows_for_log = [_normalize_final30_score_fields(normalize_final30_contract_row(dict(row or {}))) for row in rows]
    source_df = pd.DataFrame(source_rows_for_log)
    logger.info(
        "[DEBUG][FINAL30_SCORED][PRE_SAVE_NULLS] rows=%d close_null=%d ma20_null=%d ma50_null=%d ma150_null=%d atr_null=%d rs_null=%d score_null=%d score_final_null=%d",
        len(source_df),
        int(source_df["close"].isna().sum()) if "close" in source_df.columns else -1,
        int(source_df["ma20"].isna().sum()) if "ma20" in source_df.columns else -1,
        int(source_df["ma50"].isna().sum()) if "ma50" in source_df.columns else -1,
        int(source_df["ma150"].isna().sum()) if "ma150" in source_df.columns else -1,
        int(source_df["atr_pct"].isna().sum()) if "atr_pct" in source_df.columns else -1,
        int(source_df["rs_percentile"].isna().sum()) if "rs_percentile" in source_df.columns else -1,
        int(source_df["score"].isna().sum()) if "score" in source_df.columns else -1,
        int(source_df["score_final"].isna().sum()) if "score_final" in source_df.columns else -1,
    )

    payload: List[Dict[str, Any]] = []
    normalized_rows_for_log: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        entry = _build_scored_payload_row(row, as_of_date=as_of_date, idx=idx)
        if not entry:
            continue
        entry["env"] = env_n
        entry["strategy"] = strategy_n
        payload.append(entry)
        normalized_rows_for_log.append(normalize_final30_contract_row(entry.get("meta") or {}))

    if not payload:
        logger.warning("[WATCHLIST][SAVE_SCORED] no valid rows after normalization env=%s strategy=%s as_of=%s", env_n, strategy_n, as_of_date)
        return

    payload_rows = [normalize_final30_contract_row(entry.get("meta") or {}) for entry in payload]
    payload_df = pd.DataFrame(payload_rows)
    if "ma20" in payload_df.columns:
        ma20_nulls = int(payload_df["ma20"].isna().sum())
        logger.info(
            "[DB][FINAL30_SCORED][PRE_INSERT] rows=%d ma20_null=%d",
            len(payload_df),
            ma20_nulls,
        )
        if ma20_nulls > 0:
            bad_codes = payload_df.loc[payload_df["ma20"].isna(), "code"].head(10).tolist()
            raise ValueError(
                f"FINAL30_SCORED_INVALID_BEFORE_DB_INSERT strategy={strategy_n} rows={len(payload_df)} ma20_nulls={ma20_nulls} sample_codes={bad_codes} source=watchlist_result.final30_scored caller=prep.save_final30"
            )
    logger.info("[DEBUG][FINAL30_SCORED][PAYLOAD_SAMPLE] %s", payload_rows[:3])
    src_cols = sorted(source_df.columns.tolist()) if not source_df.empty else []
    payload_cols = sorted(payload_rows[0].keys()) if payload_rows else []
    logger.info(
        "[DEBUG][FINAL30_SCORED][SCHEMA] src_cols=%s payload_cols=%s",
        src_cols,
        payload_cols,
    )
    for row in payload_rows[:5]:
        logger.info(
            "[DEBUG][FINAL30_SCORED][SCORE_CHECK] code=%s score=%s score_final=%s final_score=%s tech_score=%s",
            row.get("code"),
            row.get("score"),
            row.get("score_final"),
            row.get("final_score"),
            row.get("tech_score"),
        )
        if row.get("ma20") is None and row.get("close") is not None:
            logger.warning("[FALLBACK][MA20] code=%s ma20 missing before save", row.get("code"))

    payload_key_set = sorted({str(key) for row in payload_rows for key in row.keys()})
    logger.info(
        "[WATCHLIST][DB_SAVE][FINAL30_SCORED][FIELD_CHECK] keys=%s has_ma20=%s",
        payload_key_set,
        int("ma20" in payload_key_set),
    )
    missing_save_fields = [
        field
        for field in sorted(set(FINAL30_SCORED_IDENTITY_COLS + REQUIRED_FINAL30_SCORED_COLS))
        if field not in payload_key_set
    ]
    first_row_keys = sorted(payload_rows[0].keys()) if payload_rows else []
    logger.info("[WATCHLIST][SAVE_SCORED][SAMPLE_KEYS] first_row_keys=%s", first_row_keys)
    logger.info("[WATCHLIST][SAVE_SCORED][CRITICAL_CHECK] missing=%s", missing_save_fields)
    if missing_save_fields:
        raise ValueError(f"FINAL30_SCORED_SAVE_MISSING_FIELDS:{missing_save_fields}")

    _log_scored_sample("[WATCHLIST][SAVE_SCORED][PRE_DB_SAMPLE]", normalized_rows_for_log)

    sample_meta = payload[0].get("meta") if isinstance(payload[0].get("meta"), dict) else {}
    sample_keys = sorted(sample_meta.keys())
    sample_row = {k: sample_meta.get(k) for k in CRITICAL_SCORED_COLS + ["code", "rank_final30", "score"] if k in sample_meta}
    logger.info("[DB][FINAL30_SCORED][SAVE] rows=%d", len(payload))
    logger.info("[DB][FINAL30_SCORED][SAVE_SAMPLE_KEYS] keys=%s", first_row_keys)
    logger.info("[WATCHLIST][SAVE_SCORED][SAMPLE_ROW] %s", sample_row)

    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(
            sa.delete(schema.pb1_watchlist).where(
                and_(
                    schema.pb1_watchlist.c.env == env_n,
                    schema.pb1_watchlist.c.strategy == strategy_n,
                    schema.pb1_watchlist.c.as_of == as_of_date,
                )
            )
        )
        conn.execute(sa.insert(schema.pb1_watchlist), payload)

    logger.info(
        "[WATCHLIST][SAVE] env=%s strategy=%s as_of=%s members=%s",
        env_n,
        strategy_n,
        as_of_date,
        len(payload),
    )
    if strategy_n == "pb1_watchlist_final_scored":
        roundtrip_repo = WatchlistRepo(engine)
        loaded_rows, _ = roundtrip_repo.load_watchlist_scored(
            env=env_n,
            strategy=strategy_n,
            as_of=as_of_date,
            allow_latest_fallback=False,
        )
        loaded_df = pd.DataFrame([normalize_final30_contract_row(row) for row in loaded_rows])
        required_nonnull_fields = ["close", "ma20", "ma50", "ma150", "atr_pct", "rs_percentile", "score_final"]
        nonnull_counts = {
            field: int(loaded_df[field].notna().sum()) if field in loaded_df.columns else 0
            for field in required_nonnull_fields
        }
        mismatch_counts = _roundtrip_mismatch_counts(
            payload_rows,
            loaded_rows,
            fields=required_nonnull_fields,
        )
        uniq_codes = len({str((row or {}).get("code") or "").zfill(6) for row in loaded_rows if (row or {}).get("code")})
        ma20_positive_count = sum(
            1
            for row in loaded_rows
            if safe_nullable_float((row or {}).get("ma20")) is not None and float((row or {}).get("ma20") or 0.0) > 0
        )
        score_final_nonnull = sum(1 for row in loaded_rows if safe_nullable_float((row or {}).get("score_final")) is not None)
        entry_style_selected_nonnull = sum(1 for row in loaded_rows if str((row or {}).get("entry_style_selected") or "").strip())
        score_consistent = False
        if {"score", "score_final"}.issubset(set(loaded_df.columns)):
            score_delta = (
                pd.to_numeric(loaded_df["score"], errors="coerce")
                - pd.to_numeric(loaded_df["score_final"], errors="coerce")
            ).abs().fillna(0.0)
            score_consistent = bool((score_delta < 1e-9).all())
        final_score_consistent = False
        if {"final_score", "score_final"}.issubset(set(loaded_df.columns)):
            final_score_delta = (
                pd.to_numeric(loaded_df["final_score"], errors="coerce")
                - pd.to_numeric(loaded_df["score_final"], errors="coerce")
            ).abs().fillna(0.0)
            final_score_consistent = bool((final_score_delta < 1e-9).all())
        roundtrip_ok = (
            len(loaded_rows) == 30
            and uniq_codes == 30
            and all(count == len(loaded_rows) for count in nonnull_counts.values())
            and all(count == 0 for count in mismatch_counts.values())
            and ma20_positive_count == len(loaded_rows)
            and score_final_nonnull == len(loaded_rows)
            and entry_style_selected_nonnull == len(loaded_rows)
            and score_consistent
            and final_score_consistent
        )
        logger.info(
            "[STAGE_SAVE][STRICT_FINAL30] strategy=pb1_watchlist_final_scored rows=30 roundtrip_ok=%s uniq_codes=%s ma20_positive_count=%s score_final_nonnull=%s entry_style_selected_nonnull=%s",
            int(roundtrip_ok),
            uniq_codes,
            ma20_positive_count,
            score_final_nonnull,
            entry_style_selected_nonnull,
        )
        if not roundtrip_ok:
            logger.warning(
                "[WATCHLIST][ROUNDTRIP][FAIL_DETAILS] rows=%s uniq_codes=%s ma20_positive_count=%s score_final_nonnull=%s entry_style_selected_nonnull=%s nonnull_counts=%s mismatch_counts=%s score_consistent=%s final_score_consistent=%s",
                len(loaded_rows),
                uniq_codes,
                ma20_positive_count,
                score_final_nonnull,
                entry_style_selected_nonnull,
                nonnull_counts,
                mismatch_counts,
                int(score_consistent),
                int(final_score_consistent),
            )
            raise ValueError("FINAL30_SCORED_ROUNDTRIP_VERIFY_FAILED")


def _normalize_scored_loaded_row(row: Any) -> Dict[str, Any]:
    meta = row.meta if isinstance(row.meta, dict) else {}
    scores = meta.get("scores") if isinstance(meta.get("scores"), dict) else {}
    derived = meta.get("derived") if isinstance(meta.get("derived"), dict) else {}
    minervini = meta.get("minervini") if isinstance(meta.get("minervini"), dict) else {}
    out: Dict[str, Any] = {
        "as_of": row.as_of.isoformat() if getattr(row, "as_of", None) is not None else meta.get("as_of"),
        "code": row.code,
        "rank": row.rank,
        "score": float(row.score) if row.score is not None else None,
        "meta": meta,
    }
    for key, value in meta.items():
        if key in {"code", "rank", "score", "meta"}:
            continue
        out[key] = value
    out.setdefault("code", row.code)
    out.setdefault("rank", row.rank)
    out.setdefault("score", float(row.score) if row.score is not None else None)
    normalized = normalize_final30_contract_row(out)
    normalized.setdefault("as_of", row.as_of.isoformat() if getattr(row, "as_of", None) is not None else meta.get("as_of"))
    for field, aliases in FINAL30_NUMERIC_RESTORE_ALIASES.items():
        restored = _restore_numeric_from_sources(normalized, meta, scores, derived, minervini, aliases=aliases)
        if restored is not None:
            normalized[field] = restored
            normalized_meta = normalized.get("meta") if isinstance(normalized.get("meta"), dict) else {}
            normalized_meta[field] = restored
            normalized["meta"] = normalized_meta
    if normalized.get("score_final") is not None:
        normalized["final_score"] = normalized.get("score_final")
        normalized["score"] = normalized.get("score_final")
    return normalized


def load_pb1_watchlist_codes(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date,
    ttl_days: int = 7,
    max_back_days: int = 3,
) -> List[str]:
    env_n = _norm_env(env)
    strategy_n = _norm_strategy(strategy)
    as_of_date = to_date(as_of)

    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        exact_stmt = (
            select(schema.pb1_watchlist.c.code)
            .where(
                and_(
                    schema.pb1_watchlist.c.env == env_n,
                    schema.pb1_watchlist.c.strategy == strategy_n,
                    schema.pb1_watchlist.c.as_of == as_of_date,
                )
            )
            .order_by(schema.pb1_watchlist.c.rank.asc())
        )
        exact_codes = conn.execute(exact_stmt).scalars().all()
        if exact_codes:
            normalized = [str(code).zfill(6) for code in exact_codes]
            logger.info(
                "[WATCHLIST][LOAD] env=%s strategy=%s as_of=%s members=%s",
                env_n,
                strategy_n,
                as_of_date,
                len(normalized),
            )
            return normalized

        effective_back_days = max(0, min(int(ttl_days), int(max_back_days)))
        if effective_back_days == 0:
            logger.info(
                "[WATCHLIST][LOAD] env=%s strategy=%s as_of=%s members=0",
                env_n,
                strategy_n,
                as_of_date,
            )
            return []

        min_date = as_of_date - timedelta(days=effective_back_days)
        latest_stmt = (
            select(func.max(schema.pb1_watchlist.c.as_of))
            .where(
                and_(
                    schema.pb1_watchlist.c.env == env_n,
                    schema.pb1_watchlist.c.strategy == strategy_n,
                    schema.pb1_watchlist.c.as_of <= as_of_date,
                    schema.pb1_watchlist.c.as_of >= min_date,
                )
            )
        )
        latest_as_of = conn.execute(latest_stmt).scalar()
        if latest_as_of is None:
            logger.info(
                "[WATCHLIST][LOAD] env=%s strategy=%s as_of=%s members=0",
                env_n,
                strategy_n,
                as_of_date,
            )
            return []

        latest_codes_stmt = (
            select(schema.pb1_watchlist.c.code)
            .where(
                and_(
                    schema.pb1_watchlist.c.env == env_n,
                    schema.pb1_watchlist.c.strategy == strategy_n,
                    schema.pb1_watchlist.c.as_of == latest_as_of,
                )
            )
            .order_by(schema.pb1_watchlist.c.rank.asc())
        )
        latest_codes = conn.execute(latest_codes_stmt).scalars().all()
        normalized = [str(code).zfill(6) for code in latest_codes]
        logger.info(
            "[WATCHLIST][LOAD] env=%s strategy=%s as_of=%s members=%s",
            env_n,
            strategy_n,
            latest_as_of,
            len(normalized),
        )
        return normalized


def ensure_run(
    conn: sa.Connection,
    schema: SchemaTables,
    *,
    run_id: str,
    env: str,
    run_window: str | None,
    strategy: str,
    ts: datetime,
    database_url: str,
) -> None:
    if not run_id or not strategy:
        return
    values = {
        "run_id": uuid_value_for_url(database_url, run_id),
        "env": env,
        "strategy": strategy,
        "run_window": run_window,
        "phase": None,
        "event_name": "ledger_event",
        "dry_run": False,
        "config_json": {},
        "started_at": ts,
    }
    if conn.dialect.name == "postgresql":
        insert_stmt = pg_insert(schema.runs).values(**values).on_conflict_do_nothing(index_elements=["run_id"])
    else:
        insert_stmt = sa.insert(schema.runs).values(**values)
    try:
        conn.execute(insert_stmt)
    except Exception:
        return


def ensure_run_from_context(
    conn: sa.Connection,
    schema: SchemaTables,
    ctx: RunContext,
    database_url: str,
) -> None:
    assert_uuid(ctx.run_id)
    values = {
        "run_id": uuid_value_for_url(database_url, ctx.run_id),
        "env": ctx.env,
        "strategy": ctx.strategy,
        "run_window": ctx.window,
        "phase": ctx.phase,
        "event_name": "ledger_event",
        "dry_run": ctx.dry_run,
        "workflow_run_id": str(ctx.gh_run_number) if ctx.gh_run_number else None,
        "git_sha": ctx.git_sha,
        "config_json": {},
        "started_at": ctx.started_at,
    }
    if conn.dialect.name == "postgresql":
        insert_stmt = pg_insert(schema.runs).values(**values).on_conflict_do_update(
            index_elements=["run_id"],
            set_={"workflow_run_id": values["workflow_run_id"], "git_sha": values["git_sha"]}
        )
    else:
        insert_stmt = sa.insert(schema.runs).values(**values)
    try:
        conn.execute(insert_stmt)
    except Exception:
        return


def execute_with_retry(conn, stmt, payload=None, retries: int = 5, base_sleep: float = 0.2):
    last_exc: OperationalError | None = None
    for attempt in range(retries):
        try:
            if payload is None:
                return conn.execute(stmt)
            return conn.execute(stmt, payload)
        except OperationalError as exc:
            last_exc = exc
            msg = str(exc).lower()
            if "database is locked" in msg or "busy" in msg:
                time.sleep(base_sleep * (2 ** attempt))
                continue
            raise
    if last_exc:
        raise last_exc
    raise OperationalError("database is locked", params=None, orig=None)


def _collect_json_type_paths(value: Any, *, path: str = "request_json", limit: int = 50) -> list[str]:
    results: list[str] = []

    def _walk(node: Any, prefix: str) -> None:
        if len(results) >= limit:
            return
        if isinstance(node, dict):
            for key, val in node.items():
                _walk(val, f"{prefix}.{key}")
            return
        if isinstance(node, (list, tuple, set)):
            for idx, val in enumerate(node):
                _walk(val, f"{prefix}[{idx}]")
            return
        results.append(f"{prefix}:{type(node).__name__}")

    _walk(value, path)
    return results


def _is_in_failed_transaction_error(exc: Exception) -> bool:
    message = f"{exc.__class__.__name__}:{exc}"
    if "InFailedSqlTransaction" in message:
        return True
    orig = getattr(exc, "orig", None)
    if orig and "InFailedSqlTransaction" in f"{orig.__class__.__name__}:{orig}":
        return True
    try:
        from psycopg.errors import InFailedSqlTransaction as PsycopgInFailedSqlTransaction
    except Exception:
        PsycopgInFailedSqlTransaction = None
    if PsycopgInFailedSqlTransaction and isinstance(orig or exc, PsycopgInFailedSqlTransaction):
        return True
    return False


class RunsRepo:
    _TERMINAL_STATUS_TOKENS = (
        "FINISH",
        "FINISHED",
        "COMPLETE",
        "COMPLETED",
        "SUCCESS",
        "SUCCEEDED",
        "FAIL",
        "FAILED",
        "FATAL",
        "ERROR",
        "CANCEL",
        "ABORT",
        "STOP",
        "SIGTERM",
        "SHUTDOWN",
        "SESSION_END",
        "STALE_TAKEOVER",
        "SKIP",
    )

    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)
        self._runs_column_type_cache: dict[str, tuple[str, bool]] = {}

    def _detect_runs_column_type(self, column_name: str) -> tuple[str, bool]:
        cached = self._runs_column_type_cache.get(column_name)
        if cached is not None:
            return cached

        detected = self._schema.runs.c[column_name].type.__class__.__name__.lower()
        is_textual = isinstance(self._schema.runs.c[column_name].type, (sa.String, sa.Text, sa.Unicode, sa.UnicodeText))
        try:
            inspector = sa.inspect(self.engine)
            reflected_columns = inspector.get_columns(self._schema.runs.name, schema=self._schema.runs.schema)
            for reflected in reflected_columns:
                if reflected.get("name") != column_name:
                    continue
                reflected_type = reflected.get("type")
                detected = str(reflected_type or detected).lower()
                is_textual = isinstance(reflected_type, (sa.String, sa.Text, sa.Unicode, sa.UnicodeText))
                if not is_textual:
                    is_textual = any(token in detected for token in ("text", "char", "varchar", "string"))
                break
        except Exception as exc:
            logger.warning("[DB][RUNS][TYPE_REFLECT][WARN] column=%s err=%s", column_name, exc)

        result = (detected, is_textual)
        self._runs_column_type_cache[column_name] = result
        return result

    def _normalized_runs_text_expr(self, column_name: str):
        raw = sa.cast(self._schema.runs.c[column_name], sa.Text)
        trimmed = func.btrim(raw)
        return sa.case(
            (trimmed.is_(None), None),
            (trimmed == "", None),
            (func.lower(trimmed) == "none", None),
            (func.lower(trimmed) == "null", None),
            else_=trimmed,
        )

    def _safe_runs_timestamp_expr(self, column_name: str):
        detected, is_textual = self._detect_runs_column_type(column_name)
        cast_mode = 1 if is_textual else 0
        logger.info(
            "[DB][RUNS][%s_TYPE] detected=%s cast_mode=%s",
            column_name.upper(),
            detected,
            cast_mode,
        )
        if not is_textual:
            return self._schema.runs.c[column_name], detected, cast_mode

        normalized = self._normalized_runs_text_expr(column_name)
        iso_like_prefix = r"^\d{4}-\d{2}-\d{2}"
        expr = sa.case(
            (
                and_(
                    normalized.is_not(None),
                    normalized.op("~")(iso_like_prefix),
                ),
                sa.cast(normalized, sa.DateTime(timezone=True)),
            ),
            else_=None,
        )
        return expr, detected, cast_mode

    def _find_started_today_text_fallback(
        self,
        *,
        env: str,
        strategy: str,
        run_window: str | None,
        phase: str | None,
        day_prefix: str,
    ) -> dict[str, Any] | None:
        started_at_text = self._normalized_runs_text_expr("started_at")
        conditions = [
            self._schema.runs.c.env == _norm_env(env),
            self._schema.runs.c.strategy == strategy,
            started_at_text.is_not(None),
            started_at_text.op("~")(r"^\d{4}-\d{2}-\d{2}"),
            started_at_text.like(f"{day_prefix}%"),
        ]
        if run_window is not None:
            conditions.append(self._schema.runs.c.run_window == run_window)
        if phase is not None:
            conditions.append(self._schema.runs.c.phase == phase)
        stmt = (
            select(self._schema.runs)
            .where(and_(*conditions))
            .order_by(started_at_text.asc())
            .limit(1)
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        return dict(row) if row else None

    def find_started_today(
        self,
        *,
        env: str,
        strategy: str,
        run_window: str | None = None,
        phase: str | None = None,
    ) -> dict[str, Any] | None:
        now = now_kst()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        logger.info(
            "[DB][RUNS][FIND_STARTED_TODAY][START] env=%s strategy=%s run_window=%s phase=%s",
            env,
            strategy,
            run_window,
            phase,
        )
        started_at_expr, detected, cast_mode = self._safe_runs_timestamp_expr("started_at")
        conditions = [
            self._schema.runs.c.env == _norm_env(env),
            self._schema.runs.c.strategy == strategy,
            started_at_expr.is_not(None),
            started_at_expr >= start,
            started_at_expr < end,
        ]
        if run_window is not None:
            conditions.append(self._schema.runs.c.run_window == run_window)
        if phase is not None:
            conditions.append(self._schema.runs.c.phase == phase)
        stmt = (
            select(self._schema.runs)
            .where(and_(*conditions))
            .order_by(started_at_expr.asc())
            .limit(1)
        )
        try:
            with self.engine.begin() as conn:
                row = conn.execute(stmt).mappings().first()
        except Exception as exc:
            logger.exception(
                "[DB][RUNS][FIND_STARTED_TODAY][FAIL] err_type=%s detected=%s cast_mode=%s err=%s",
                exc.__class__.__name__,
                detected,
                cast_mode,
                exc,
            )
            if cast_mode != 1:
                raise
            row_dict = self._find_started_today_text_fallback(
                env=env,
                strategy=strategy,
                run_window=run_window,
                phase=phase,
                day_prefix=start.date().isoformat(),
            )
            logger.info(
                "[DB][RUNS][FIND_STARTED_TODAY][OK] found=%s run_id=%s fallback=text_prefix",
                1 if row_dict else 0,
                (row_dict or {}).get("run_id"),
            )
            return row_dict
        row_dict = dict(row) if row else None
        logger.info(
            "[DB][RUNS][FIND_STARTED_TODAY][OK] found=%s run_id=%s",
            1 if row_dict else 0,
            (row_dict or {}).get("run_id"),
        )
        return row_dict

    @classmethod
    def _is_terminal_status(cls, status: Any) -> bool:
        normalized = str(status or "").strip().upper()
        if not normalized:
            return False
        return any(token in normalized for token in cls._TERMINAL_STATUS_TOKENS)

    @staticmethod
    def _coerce_run_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time(), tzinfo=pytz.timezone("Asia/Seoul"))
        raw = str(value).strip()
        if not raw or raw.lower() in {"none", "null"}:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
        if parsed.tzinfo is None:
            return pytz.timezone("Asia/Seoul").localize(parsed)
        return parsed

    @classmethod
    def _last_activity_at(cls, run_row: dict[str, Any] | None) -> datetime | None:
        row = run_row or {}
        for field in ("heartbeat_at", "updated_at", "started_at"):
            parsed = cls._coerce_run_datetime(row.get(field))
            if parsed is not None:
                return parsed
        return None

    def find_active_session_today(
        self,
        *,
        env: str,
        strategy: str,
        run_window: str | None = None,
        phase: str | None = None,
    ) -> dict[str, Any] | None:
        now = now_kst()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        started_at_expr, _, _ = self._safe_runs_timestamp_expr("started_at")
        heartbeat_expr, _, _ = self._safe_runs_timestamp_expr("heartbeat_at")
        updated_expr, _, _ = self._safe_runs_timestamp_expr("updated_at")
        finished_expr, _, _ = self._safe_runs_timestamp_expr("finished_at")
        status_upper = func.upper(func.coalesce(self._schema.runs.c.status, ""))
        conditions = [
            self._schema.runs.c.env == _norm_env(env),
            self._schema.runs.c.strategy == strategy,
            started_at_expr.is_not(None),
            started_at_expr >= start,
            started_at_expr < end,
            finished_expr.is_(None),
        ]
        for token in self._TERMINAL_STATUS_TOKENS:
            conditions.append(~status_upper.contains(token))
        if run_window is not None:
            conditions.append(self._schema.runs.c.run_window == run_window)
        if phase is not None:
            conditions.append(self._schema.runs.c.phase == phase)
        stmt = (
            select(self._schema.runs)
            .where(and_(*conditions))
            .order_by(func.coalesce(heartbeat_expr, updated_expr, started_at_expr).desc(), started_at_expr.desc())
            .limit(1)
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        return dict(row) if row else None

    def is_session_stale(self, run_row: dict[str, Any] | None, *, stale_sec: int) -> bool:
        row = run_row or {}
        if not row:
            return False
        if self._is_terminal_status(row.get("status")):
            return True
        if row.get("finished_at") is not None:
            return True
        reference_time = self._last_activity_at(row)
        if reference_time is None:
            return True
        now = now_kst()
        if reference_time.tzinfo is None:
            reference_time = pytz.timezone("Asia/Seoul").localize(reference_time)
        age_sec = max(0, int((now - reference_time.astimezone(now.tzinfo)).total_seconds()))
        return age_sec >= max(0, int(stale_sec))

    def touch_run(self, run_id: str, *, now: datetime | None = None, status: str | None = None) -> None:
        stamp = now or func.now()
        values: dict[str, Any] = {
            "heartbeat_at": stamp,
            "updated_at": stamp,
        }
        if status:
            values["status"] = status
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.runs)
                .where(self._schema.runs.c.run_id == run_id)
                .values(**values),
            )

    def mark_run_abandoned(
        self,
        run_id: str,
        *,
        reason: str,
        takeover_from_run_id: str | None = None,
        status: str = "SESSION_ABORTED",
    ) -> None:
        takeover_from_run_id = _normalize_run_id_text(takeover_from_run_id)
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.runs)
                .where(self._schema.runs.c.run_id == run_id)
                .values(
                    status=status,
                    aborted_reason=reason,
                    takeover_from_run_id=takeover_from_run_id,
                    finished_at=func.now(),
                    heartbeat_at=func.now(),
                    updated_at=func.now(),
                ),
            )

    def finish_stale_session_if_needed(
        self,
        run_row: dict[str, Any] | None,
        *,
        stale_sec: int,
        reason: str,
        takeover_from_run_id: str | None = None,
        status: str = "SESSION_STALE_TAKEOVER",
    ) -> bool:
        if not run_row:
            return False
        if not self.is_session_stale(run_row, stale_sec=stale_sec):
            return False
        run_id = str((run_row or {}).get("run_id") or "").strip()
        if not run_id:
            return False
        self.mark_run_abandoned(
            run_id,
            reason=reason,
            takeover_from_run_id=takeover_from_run_id,
            status=status,
        )
        return True

    def create_session_guard(
        self,
        *,
        env: str,
        strategy: str,
        run_window: str,
        phase: str,
        event_name: str,
        workflow: str | None,
        workflow_run_id: str | None,
        workflow_attempt: int | None,
        git_sha: str | None,
        config_json: dict | None = None,
        takeover_from_run_id: str | None = None,
    ) -> str:
        takeover_from_run_id = _normalize_run_id_text(takeover_from_run_id)
        values = {
            "run_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
            "env": env,
            "strategy": strategy,
            "run_window": run_window,
            "phase": phase,
            "event_name": event_name,
            "dry_run": False,
            "git_sha": git_sha,
            "workflow": workflow,
            "workflow_run_id": workflow_run_id,
            "workflow_attempt": workflow_attempt,
            "config_json": config_json or {},
            "status": "SESSION_GUARD_STARTED",
            "heartbeat_at": func.now(),
            "updated_at": func.now(),
            "takeover_from_run_id": takeover_from_run_id,
        }
        stmt = sa.insert(self._schema.runs).values(**values).returning(self._schema.runs.c.run_id)
        with self.engine.begin() as conn:
            run_id = conn.execute(stmt).scalar() or values["run_id"]
        return str(run_id)

    def start_run(
        self,
        env: str,
        strategy: str,
        run_window: str | None,
        phase: str,
        event_name: str,
        dry_run: bool,
        git_sha: str | None,
        workflow: str | None,
        workflow_run_id: str | None,
        workflow_attempt: int | None,
        config_json: dict | None,
    ) -> str:
        # Handle dry_run type compatibility: try bool first, fallback to int if column is INTEGER
        dry_run_value = dry_run
        values = {
            "run_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
            "env": env,
            "strategy": strategy,
            "run_window": run_window,
            "phase": phase,
            "event_name": event_name,
            "dry_run": dry_run_value,
            "git_sha": git_sha,
            "workflow": workflow,
            "workflow_run_id": workflow_run_id,
            "workflow_attempt": workflow_attempt,
            "config_json": config_json or {},
        }
        stmt = sa.insert(self._schema.runs).values(**values)
        run_id = values["run_id"]
        for attempt in range(2):
            with self.engine.begin() as conn:
                try:
                    res = conn.execute(stmt.returning(self._schema.runs.c.run_id))
                    run_id = res.scalar() or run_id
                    return str(run_id)
                except OperationalError:
                    raise
                except Exception as e:
                    # If type mismatch, try with int conversion
                    if "dry_run" in str(e) and isinstance(dry_run_value, bool):
                        dry_run_value = int(dry_run)
                        values["dry_run"] = dry_run_value
                        stmt = sa.insert(self._schema.runs).values(**values)
                        conn.rollback()  # Rollback failed transaction
                        res = conn.execute(stmt.returning(self._schema.runs.c.run_id))
                        run_id = res.scalar() or run_id
                        return str(run_id)
                    else:
                        raise
        return str(run_id)

    def finish_run(
        self,
        run_id: str,
        status: str,
        notes: str | None = None,
        *,
        aborted_reason: str | None = None,
        takeover_from_run_id: str | None = None,
    ) -> None:
        takeover_from_run_id = _normalize_run_id_text(takeover_from_run_id)
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.runs)
                .where(self._schema.runs.c.run_id == run_id)
                .values(
                    status=status,
                    finished_at=func.now(),
                    heartbeat_at=func.now(),
                    updated_at=func.now(),
                    notes=notes,
                    aborted_reason=aborted_reason,
                    takeover_from_run_id=takeover_from_run_id,
                ),
            )

    def upsert_run(
        self,
        run_id: str,
        env: str,
        strategy: str,
        workflow_run_id: str | None = None,
        ts_start: datetime | None = None,
        mode: str | None = None,
        **kwargs
    ) -> None:
        values = {
            "run_id": _coerce_uuid(run_id, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
            "env": env,
            "strategy": strategy,
            "workflow_run_id": workflow_run_id,
            "started_at": ts_start or func.now(),
            "status": "STARTED",
            **kwargs
        }
        with self.engine.begin() as conn:
            if conn.dialect.name == "postgresql":
                # Use PostgreSQL-specific upsert
                pk_cols = list(self._schema.runs.primary_key.columns)
                update_map = {k: pg_insert(self._schema.runs).excluded[k] 
                             for k in values.keys() if k not in {col.name for col in pk_cols}}
                stmt = pg_insert(self._schema.runs).values(**values).on_conflict_do_update(
                    index_elements=pk_cols,
                    set_=update_map
                )
                conn.execute(stmt)
            else:
                # Fallback upsert for non-PostgreSQL dialects (SQLite, etc.)
                try:
                    conn.execute(sa.insert(self._schema.runs).values(**values))
                except IntegrityError:
                    # If insert fails, update existing row
                    pk_cols = list(self._schema.runs.primary_key.columns)
                    update_values = {k: v for k, v in values.items() 
                                    if k not in {col.name for col in pk_cols}}
                    where_clause = and_(
                        *[self._schema.runs.c[col.name] == values[col.name] for col in pk_cols]
                    )
                    conn.execute(
                        sa.update(self._schema.runs).where(where_clause).values(**update_values)
                    )

    def ensure_run_exists(self, run_id: str) -> None:
        # Check if run exists, if not, insert minimal row
        stmt = sa.select(self._schema.runs.c.run_id).where(self._schema.runs.c.run_id == run_id)
        with self.engine.begin() as conn:
            exists = conn.execute(stmt).first() is not None
            if not exists:
                # Insert minimal row
                values = {
                    "run_id": _coerce_uuid(run_id, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                    "env": "unknown",
                    "strategy": "unknown",
                    "status": "STARTED",
                }
                conn.execute(sa.insert(self._schema.runs).values(**values))


class UniverseRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def _strategy_key(self, env: str, strategy: str) -> str:
        return f"{env}:{strategy}"

    def _fetch_members_for_run(self, run_id: str, *, env: str, strategy: str) -> list[dict]:
        stmt = (
            select(
                self._schema.universe_members.c.stock_code,
                self._schema.universe_members.c.name,
                self._schema.universe_members.c.market,
                self._schema.universe_members.c.rank,
                self._schema.universe_members.c.market_cap,
                self._schema.universe_members.c.reason,
                self._schema.universe_runs.c.as_of,
                self._schema.universe_runs.c.provider,
            )
            .select_from(
                self._schema.universe_members.join(
                    self._schema.universe_runs,
                    self._schema.universe_members.c.run_id == self._schema.universe_runs.c.run_id,
                )
            )
            .where(self._schema.universe_members.c.run_id == run_id)
            .order_by(self._schema.universe_members.c.rank.nullsfirst())
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
            results: list[dict] = []
            for row in rows:
                payload = dict(row)
                payload["code"] = payload.pop("stock_code")
                payload["as_of_date"] = payload.pop("as_of")
                payload["env"] = env
                payload["strategy"] = strategy
                results.append(payload)
            return results

    def get_current_universe_members(self, env: str, strategy: str) -> list[dict]:
        strategy_key = self._strategy_key(env, strategy)
        stmt = select(self._schema.universe_current.c.run_id).where(self._schema.universe_current.c.strategy == strategy_key)
        with self.engine.begin() as conn:
            run_id = conn.execute(stmt).scalar()
        if not run_id:
            return []
        return self._fetch_members_for_run(str(run_id), env=env, strategy=strategy)

    def get_current_universe_snapshot(self, env: str, strategy: str) -> dict | None:
        strategy_key = self._strategy_key(env, strategy)
        members_count_sq = (
            select(
                self._schema.universe_members.c.run_id,
                func.count().label("members_count"),
            )
            .group_by(self._schema.universe_members.c.run_id)
            .subquery()
        )
        stmt = (
            select(
                self._schema.universe_current.c.run_id,
                self._schema.universe_runs.c.as_of,
                self._schema.universe_runs.c.provider,
                self._schema.universe_runs.c.status,
                members_count_sq.c.members_count,
            )
            .select_from(
                self._schema.universe_current.join(
                    self._schema.universe_runs,
                    self._schema.universe_current.c.run_id == self._schema.universe_runs.c.run_id,
                ).outerjoin(
                    members_count_sq,
                    members_count_sq.c.run_id == self._schema.universe_runs.c.run_id,
                )
            )
            .where(self._schema.universe_current.c.strategy == strategy_key)
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
        if not row:
            return None
        run_id = str(row["run_id"])
        members = self._fetch_members_for_run(run_id, env=env, strategy=strategy)
        return {
            "run_id": run_id,
            "as_of": row.get("as_of"),
            "provider": row.get("provider"),
            "status": row.get("status"),
            "members_count": len(members),
            "members": members,
            "sample_codes": [m.get("code") for m in members[:5]],
        }

    def get_latest_universe_members(
        self,
        *,
        env: str,
        strategy: str,
        as_of_date: str,
    ) -> list[dict]:
        """Get the most recent universe members for as_of <= as_of_date."""
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        stmt = (
            select(self._schema.universe_runs.c.run_id)
            .where(
                and_(
                    self._schema.universe_runs.c.strategy == strategy_key,
                    self._schema.universe_runs.c.as_of <= as_of_d,
                )
            )
            .order_by(self._schema.universe_runs.c.as_of.desc())
            .limit(1)
        )
        with self.engine.begin() as conn:
            run_id = conn.execute(stmt).scalar()
        if not run_id:
            return []
        return self._fetch_members_for_run(str(run_id), env=env, strategy=strategy)
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        stmt = (
            select(
                self._schema.universe_runs.c.run_id,
                self._schema.universe_runs.c.as_of,
                self._schema.universe_runs.c.provider,
                self._schema.universe_runs.c.status,
            )
            .where(
                and_(
                    self._schema.universe_runs.c.strategy == strategy_key,
                    self._schema.universe_runs.c.as_of <= as_of_d,
                    or_(
                        self._schema.universe_runs.c.status == "SUCCESS",
                        self._schema.universe_runs.c.status.is_(None),
                    ),
                )
            )
            .order_by(self._schema.universe_runs.c.as_of.desc(), self._schema.universe_runs.c.created_ts.desc())
            .limit(limit)
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        for row in rows:
            run_id = str(row["run_id"])
            members = self._fetch_members_for_run(run_id, env=env, strategy=strategy)
            if members:
                return {
                    "run_id": run_id,
                    "as_of": row.get("as_of"),
                    "provider": row.get("provider"),
                    "status": row.get("status"),
                    "members_count": len(members),
                    "members": members,
                    "sample_codes": [m.get("code") for m in members[:5]],
                }
        return None

    def get_universe_members(
        self,
        *,
        env: str,
        strategy: str,
        as_of_date: str,
        allow_fallback: bool = True,
    ) -> list[dict]:
        """
        Get universe members for given (env, strategy, as_of_date).
        
        CRITICAL: env is account_env (practice/paper/real), NOT exec_mode.
        Uses (strategy_key, as_of) tuple only - NO run_id filtering.
        """
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        stmt = (
            select(self._schema.universe_runs.c.run_id)
            .where(
                and_(
                    self._schema.universe_runs.c.strategy == strategy_key,
                    self._schema.universe_runs.c.as_of == as_of_d,
                )
            )
            .order_by(self._schema.universe_runs.c.created_ts.desc())
        )
        with self.engine.begin() as conn:
            run_id = conn.execute(stmt).scalar()
        if not run_id:
            if not allow_fallback:
                logger.info(
                    "[UNIVERSE][DB][LOAD][MISS] env=%s strategy=%s as_of=%s members=0",
                    env,
                    strategy,
                    as_of_date,
                )
                return []
            logger.info(
                "[UNIVERSE][DB][LOAD] env=%s strategy=%s as_of=%s members=0 (trying fallback)",
                env, strategy, as_of_date
            )
            # [PATCH] Fallback to latest non-empty as_of
            fallback_stmt = (
                select(self._schema.universe_runs.c.as_of, self._schema.universe_runs.c.run_id)
                .where(self._schema.universe_runs.c.strategy == strategy_key)
                .order_by(self._schema.universe_runs.c.as_of.desc())
            )
            with self.engine.begin() as conn:
                result = conn.execute(fallback_stmt)
                candidates = result.mappings().all()
            for row in candidates:
                fallback_run_id = str(row["run_id"])
                fallback_members = self._fetch_members_for_run(fallback_run_id, env=env, strategy=strategy)
                if fallback_members:
                    logger.info(
                        "[UNIVERSE][DB][LOAD][FALLBACK] env=%s strategy=%s as_of=%s -> %s members=%s",
                        env, strategy, as_of_date, row["as_of"], len(fallback_members)
                    )
                    return fallback_members
            logger.info(
                "[UNIVERSE][DB][LOAD] env=%s strategy=%s as_of=%s members=0 (no fallback)",
                env, strategy, as_of_date
            )
            return []
        members = self._fetch_members_for_run(str(run_id), env=env, strategy=strategy)
        if not members:
            if not allow_fallback:
                logger.info(
                    "[UNIVERSE][DB][LOAD][MISS] env=%s strategy=%s as_of=%s members=0",
                    env,
                    strategy,
                    as_of_date,
                )
                return []
            logger.info(
                "[UNIVERSE][DB][LOAD] env=%s strategy=%s as_of=%s members=0 (trying fallback)",
                env, strategy, as_of_date
            )
            # [PATCH] Fallback to latest non-empty as_of
            fallback_stmt = (
                select(self._schema.universe_runs.c.as_of, self._schema.universe_runs.c.run_id)
                .where(
                    and_(
                        self._schema.universe_runs.c.strategy == strategy_key,
                        self._schema.universe_runs.c.as_of < as_of_d,
                    )
                )
                .order_by(self._schema.universe_runs.c.as_of.desc())
            )
            with self.engine.begin() as conn:
                result = conn.execute(fallback_stmt)
                candidates = result.mappings().all()
            for row in candidates:
                fallback_run_id = str(row["run_id"])
                fallback_members = self._fetch_members_for_run(fallback_run_id, env=env, strategy=strategy)
                if fallback_members:
                    logger.info(
                        "[UNIVERSE][DB][LOAD][FALLBACK] env=%s strategy=%s as_of=%s -> %s members=%s",
                        env, strategy, as_of_date, row["as_of"], len(fallback_members)
                    )
                    return fallback_members
            logger.info(
                "[UNIVERSE][DB][LOAD] env=%s strategy=%s as_of=%s members=0 (no fallback)",
                env, strategy, as_of_date
            )
        else:
            logger.info(
                "[UNIVERSE][DB][LOAD] env=%s strategy=%s as_of=%s members=%s",
                env, strategy, as_of_date, len(members)
            )
        return members

    def save_universe_run_and_members(
        self,
        *,
        env: str,
        strategy: str,
        as_of: str,
        members: list[dict],
    ) -> None:
        """Upsert universe run and members in a transaction."""
        # Use the new idempotent flow: start -> store
        run_id = self.start_universe_run(
            env=env,
            strategy=strategy,
            as_of_date=as_of,
            provider="auto_build",
        )
        self.store_universe_snapshot(
            run_id=run_id,
            env=env,
            strategy=strategy,
            as_of_date=as_of,
            provider="auto_build",
            members=members,
            reason="auto_build_from_ensure",
        )

    def start_universe_run(
        self,
        *,
        env: str,
        strategy: str,
        as_of_date: str,
        provider: str,
        requested_as_of: str | None = None,
        actual_as_of: str | None = None,
        build_reason: str | None = None,
        universe_name: str | None = None,
    ) -> str:
        """
        Idempotent start:
        - universe_runs has UNIQUE(strategy, provider, as_of) (ux_universe_runs_key)
        - If already exists, REUSE run_id and UPDATE status to RUNNING
        - Else INSERT new row
        """
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        requested_as_of_d = _as_date(requested_as_of or as_of_date)
        actual_as_of_d = _as_date(actual_as_of or as_of_date)
        now = now_kst().isoformat()
        
        try:
            with self.engine.begin() as conn:
                # Check if run already exists for this (strategy, provider, as_of) combination
                existing = conn.execute(
                    sa.select(self._schema.universe_runs.c.run_id)
                      .where(self._schema.universe_runs.c.strategy == strategy_key)
                      .where(self._schema.universe_runs.c.provider == provider)
                      .where(self._schema.universe_runs.c.as_of == as_of_d)
                      .limit(1)
                ).scalar_one_or_none()
                
                if existing:
                    # Same as_of re-run → reuse existing run_id + update status only
                    conn.execute(
                        sa.update(self._schema.universe_runs)
                          .where(self._schema.universe_runs.c.run_id == existing)
                          .values(
                              created_ts=now,
                              status="RUNNING",
                              members_count=0,
                              error_reason=None,
                              requested_as_of=requested_as_of_d,
                              actual_as_of=actual_as_of_d,
                              build_reason=build_reason,
                              universe_name=universe_name or strategy_key,
                          )
                    )
                    logger.info(
                        "[UNIVERSE][RUN][RESTART] run_id=%s env=%s strategy=%s as_of=%s (reusing existing)",
                        existing, env, strategy, as_of_date
                    )
                    return str(existing)
                
                # New run - insert
                run_id = _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url))
                conn.execute(
                    sa.insert(self._schema.universe_runs).values(
                        run_id=run_id,
                        strategy=strategy_key,
                        provider=provider,
                        requested_as_of=requested_as_of_d,
                        actual_as_of=actual_as_of_d,
                        build_reason=build_reason,
                        universe_name=universe_name or strategy_key,
                        as_of=as_of_d,
                        created_ts=now,
                        status="RUNNING",
                        members_count=0,
                    )
                )
                logger.info("[UNIVERSE][RUN][START] run_id=%s env=%s strategy=%s as_of=%s", run_id, env, strategy, as_of_date)
                return str(run_id)
        except Exception:
            logger.exception("[UNIVERSE][RUN][START_FAIL] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
            raise
        run_id = _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url))
        strategy_key = self._strategy_key(env, strategy)
        as_of_d = _as_date(as_of_date)
        try:
            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self._schema.universe_runs.c.run_id).where(
                        and_(
                            self._schema.universe_runs.c.strategy == strategy_key,
                            self._schema.universe_runs.c.provider == provider,
                            self._schema.universe_runs.c.as_of == as_of_d,
                        )
                    )
                ).scalar()
                if existing:
                    conn.execute(sa.delete(self._schema.universe_runs).where(self._schema.universe_runs.c.run_id == existing))
                conn.execute(
                    sa.insert(self._schema.universe_runs).values(
                        run_id=run_id,
                        strategy=strategy_key,
                        provider=provider,
                        as_of=as_of_d,
                        created_ts=now_kst().isoformat(),
                        status="FAIL",
                        error_reason=error_reason,
                        members_count=members_count,
                    )
                )
            return str(run_id)
        except Exception:
            logger.exception("[UNIVERSE][RUN][FAIL_LOG] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
            raise

    def cleanup_old_runs(self, *, retain_days: int = 30) -> None:
        cutoff = (now_kst() - timedelta(days=retain_days)).isoformat()
        with self.engine.begin() as conn:
            current_runs = select(self._schema.universe_current.c.run_id)
            conn.execute(
                sa.delete(self._schema.universe_runs).where(
                    and_(
                        self._schema.universe_runs.c.created_ts < cutoff,
                        self._schema.universe_runs.c.run_id.not_in(current_runs),
                    )
                )
            )

    def store_universe_snapshot(
        self,
        *,
        run_id: str,
        env: str,
        strategy: str,
        as_of_date: str,
        provider: str,
        members: list[dict],
        reason: str | None = None,
        requested_as_of: str | None = None,
        actual_as_of: str | None = None,
        build_reason: str | None = None,
        universe_name: str | None = None,
    ) -> None:
        """
        Store universe snapshot to DB (UNIVERSE_RUNS + UNIVERSE_MEMBERS + UNIVERSE_CURRENT).
        Updates existing run_id with SUCCESS status (does NOT insert new run).
        """
        members_list = list(members)
        as_of_d = _as_date(as_of_date)
        requested_as_of_d = _as_date(requested_as_of or as_of_date)
        actual_as_of_d = _as_date(actual_as_of or as_of_date)
        db_url = str(self.engine.url)
        strategy_key = self._strategy_key(env, strategy)
        members_count = len(members_list)
        now = now_kst().isoformat()
        
        try:
            with self.engine.begin() as conn:
                # (A) Update universe_runs with SUCCESS (NOT INSERT)
                conn.execute(
                    sa.update(self._schema.universe_runs)
                      .where(self._schema.universe_runs.c.run_id == uuid_value_for_url(db_url, run_id))
                      .values(
                          status="SUCCESS",
                          members_count=members_count,
                          error_reason=None,
                          requested_as_of=requested_as_of_d,
                          actual_as_of=actual_as_of_d,
                          build_reason=build_reason or reason,
                          universe_name=universe_name or strategy_key,
                          created_ts=now,
                      )
                )
                
                # (B) Insert members - REPLACE strategy (DELETE old + INSERT new)
                # Check preexisting members for debugging
                cnt = conn.execute(
                    sa.select(sa.func.count()).select_from(self._schema.universe_members)
                      .where(self._schema.universe_members.c.run_id == uuid_value_for_url(db_url, run_id))
                ).scalar_one()
                logger.info(f"[UNIVERSE][STORE] preexisting members for run_id={run_id}: {cnt}")
                
                # ✅ RESTART/재실행 대비: 기존 멤버 싹 지우고 다시 넣기
                conn.execute(
                    sa.delete(self._schema.universe_members)
                      .where(self._schema.universe_members.c.run_id == uuid_value_for_url(db_url, run_id))
                )
                
                # Now insert fresh members
                for rank, member in enumerate(members_list, start=1):
                    meta = member.get("meta_json") or {}
                    conn.execute(
                        sa.insert(self._schema.universe_members).values(
                            run_id=uuid_value_for_url(db_url, run_id),
                            stock_code=str(member.get("code") or "").zfill(6),
                            name=member.get("name") or meta.get("name"),
                            market=member.get("market"),
                            rank=member.get("rank") if member.get("rank") is not None else rank,
                            market_cap=member.get("market_cap") or meta.get("market_cap") or meta.get("mktcap"),
                            reason=member.get("reason") or reason,
                        )
                    )
                
                # (C) Update UNIVERSE_CURRENT pointer (UPSERT) - this is FK-safe
                if members_count > 0:
                    if self.engine.dialect.name == "postgresql":
                        insert_stmt = pg_insert(self._schema.universe_current).values(
                            strategy=strategy_key,
                            run_id=uuid_value_for_url(db_url, run_id),
                            updated_ts=now,
                        ).on_conflict_do_update(
                            index_elements=[self._schema.universe_current.c.strategy],
                            set_={
                                "run_id": uuid_value_for_url(db_url, run_id),
                                "updated_ts": now,
                            },
                        )
                        conn.execute(insert_stmt)
                    else:
                        # SQLite/others: delete + insert
                        conn.execute(
                            sa.delete(self._schema.universe_current).where(
                                self._schema.universe_current.c.strategy == strategy_key
                            )
                        )
                        conn.execute(
                            sa.insert(self._schema.universe_current).values(
                                strategy=strategy_key,
                                run_id=uuid_value_for_url(db_url, run_id),
                                updated_ts=now,
                            )
                        )
            
            logger.info(
                "[UNIVERSE][STORE][OK] env=%s strategy=%s as_of=%s members=%s run_id=%s",
                env, strategy, as_of_date, members_count, run_id
            )
        except Exception:
            logger.exception(
                "[UNIVERSE][STORE][FAIL] env=%s strategy=%s as_of=%s run_id=%s",
                env, strategy, as_of_date, run_id
            )
            raise

    def record_universe_run_failure(
        self,
        *,
        run_id: str,
        env: str,
        strategy: str,
        as_of_date: str,
        provider: str,
        error_reason: str,
        members_count: int = 0,
    ) -> None:
        """
        Record universe build failure in UNIVERSE_RUNS.
        Updates existing run_id with FAILED status (does NOT insert new run).
        This method MUST NOT raise exceptions to prevent double-failure.
        """
        try:
            db_url = str(self.engine.url)
            now = now_kst().isoformat()
            
            with self.engine.begin() as conn:
                # Update failure record (NOT INSERT)
                conn.execute(
                    sa.update(self._schema.universe_runs)
                      .where(self._schema.universe_runs.c.run_id == uuid_value_for_url(db_url, run_id))
                      .values(
                          status="FAILED",
                          error_reason=error_reason,
                          members_count=members_count,
                          created_ts=now,
                      )
                )
            
            logger.info(
                "[UNIVERSE][RUN][FAIL_RECORDED] env=%s strategy=%s as_of=%s run_id=%s reason=%s",
                env, strategy, as_of_date, run_id, error_reason
            )
        except Exception:
            # CRITICAL: Never raise from this method to prevent double-failure
            logger.error(
                "[UNIVERSE][RUN][FAIL_LOG_ERROR] env=%s strategy=%s as_of=%s run_id=%s error=%s",
                env, strategy, as_of_date, run_id, error_reason,
                exc_info=True
            )


class OrdersRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)
        self._last_read_fail_open_op: str | None = None

    def _read_mappings_with_guard(
        self,
        stmt,
        *,
        op_name: str,
        fail_open: bool | None = None,
    ) -> list[dict]:
        if fail_open is None:
            fail_open = _order_lookup_fail_open_default()

        self._last_read_fail_open_op = None
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(stmt).mappings().all()
                logger.info("[DB][READ][OK] op=%s rows=%s fail_open=%s", op_name, len(rows), int(bool(fail_open)))
                return [dict(r) for r in rows]
        except (OperationalError, DBAPIError, SATimeoutError, StatementError) as exc:
            logger.exception(
                "[DB][READ][FAIL] op=%s fail_open=%s err_type=%s err=%s",
                op_name,
                int(bool(fail_open)),
                type(exc).__name__,
                exc,
            )
            if fail_open:
                self._last_read_fail_open_op = op_name
                logger.warning("[DB][READ][FAIL_OPEN] op=%s -> returning []", op_name)
                return []
            raise

    def consume_fail_open_marker(self, op_name: str) -> bool:
        if self._last_read_fail_open_op != op_name:
            return False
        self._last_read_fail_open_op = None
        return True

    def _window_expr(self, column):
        if self.engine.dialect.name == "postgresql":
            return sa.cast(column, sa.DateTime(timezone=True))
        return column

    def ensure_run_exists(self, run_id: str) -> None:
        # Check if run exists, if not, insert minimal row
        stmt = sa.select(self._schema.runs.c.run_id).where(self._schema.runs.c.run_id == run_id)
        with self.engine.begin() as conn:
            exists = conn.execute(stmt).first() is not None
            if not exists:
                # Insert minimal row
                values = {
                    "run_id": _coerce_uuid(run_id, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                    "env": "unknown",
                    "strategy": "unknown",
                    "status": "STARTED",
                }
                conn.execute(sa.insert(self._schema.runs).values(**values))

    def create_intent_idempotent(
        self,
        env: str,
        run_id: str,
        strategy: str,
        sid: int,
        mode: int,
        code: str,
        market: str | None,
        side: str,
        ord_type: str,
        qty: int,
        limit_price: float | None,
        stage: str,
        client_order_key: str,
        request_json: dict | None,
        *,
        status: str = "CREATED",
        entry_meta_json: dict | None = None,
    ) -> tuple[str, bool]:
        db_url = str(self.engine.url)
        original_request_json = request_json
        if run_id:
            self.ensure_run_exists(run_id)
        payload = {
            "order_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": env,
            "run_id": uuid_value_for_url(db_url, run_id) if run_id is not None else None,
            "strategy": strategy,
            "sid": sid,
            "mode": mode,
            "code": code,
            "market": market,
            "side": side,
            "ord_type": ord_type,
            "qty": qty,
            "limit_price": limit_price,
            "stage": stage,
            "client_order_key": client_order_key,
            "status": status,
            "request_json": request_json or {},
            **_entry_meta_columns(entry_meta_json, json_field="entry_meta_json"),
        }
        payload = dict(payload)
        if "request_json" in payload:
            payload["request_json"] = json_sanitize(payload["request_json"])
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(self._schema.orders.c.order_id).where(
                    and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key)
                )
            ).scalar()
            if existing:
                if entry_meta_json:
                    conn.execute(
                        sa.update(self._schema.orders)
                        .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                        .values(**_entry_meta_columns(entry_meta_json, json_field="entry_meta_json"), updated_at=func.now())
                    )
                return str(existing), False
            stmt = sa.insert(self._schema.orders).values(**payload).returning(self._schema.orders.c.order_id)
            try:
                res = execute_with_retry(conn, stmt)
                return str(res.scalar()), True
            except StatementError as exc:
                if original_request_json is not None:
                    type_paths = _collect_json_type_paths(original_request_json)
                    logger.warning(
                        "[DB][ORDER_INTENT][REQUEST_JSON_TYPES] env=%s key=%s types=%s",
                        env,
                        client_order_key,
                        type_paths,
                    )
                raise exc
            except Exception:
                execute_with_retry(conn, sa.insert(self._schema.orders).values(**payload))
                return str(payload["order_id"]), True

    def mark_submitted(
        self,
        env: str,
        client_order_key: str,
        kis_odno: str | None,
        response_json: dict | None,
        *,
        entry_meta_json: dict | None = None,
    ) -> None:
        safe_response_json = json_sanitize(response_json or {})
        values = {
            "status": "SUBMITTED",
            "kis_odno": kis_odno,
            "broker_order_id": kis_odno or client_order_key,
            "response_json": safe_response_json,
            "submitted_at": func.now(),
            "updated_at": func.now(),
        }
        if entry_meta_json:
            values.update(_entry_meta_columns(entry_meta_json, json_field="entry_meta_json"))
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(**values)
            )

    def mark_acked(
        self,
        env: str,
        kis_odno: str | None,
        response_json: dict | None,
        *,
        entry_meta_json: dict | None = None,
    ) -> None:
        safe_response_json = json_sanitize(response_json or {})
        values = {
            "status": "ACKED",
            "response_json": safe_response_json,
            "broker_order_id": kis_odno,
            "acked_at": func.now(),
            "updated_at": func.now(),
        }
        if entry_meta_json:
            values.update(_entry_meta_columns(entry_meta_json, json_field="entry_meta_json"))
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.kis_odno == kis_odno))
                .values(**values)
            )

    def mark_error(self, env: str, client_order_key: str, error_payload: dict | None) -> None:
        safe_error_payload = json_sanitize(error_payload or {})
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(status="ERROR", response_json=safe_error_payload, updated_at=func.now()),
            )

    def mark_cancelled(self, env: str, client_order_key: str, response_json: dict | None = None) -> None:
        safe_response_json = json_sanitize(response_json or {})
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(self._schema.orders)
                .where(and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key))
                .values(status="CANCELLED", response_json=safe_response_json, updated_at=func.now()),
            )

    def get_open_orders(self, env: str) -> list[dict]:
        stmt = select(self._schema.orders).where(
            and_(self._schema.orders.c.env == env, self._schema.orders.c.status.in_(["INTENT", "SUBMITTED"]))
        )
        return self._read_mappings_with_guard(
            stmt,
            op_name="orders.get_open_orders",
            fail_open=None,
        )

    def has_client_order_key(self, env: str, client_order_key: str) -> bool:
        stmt = select(self._schema.orders.c.order_id.label("order_id")).where(
            and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key)
        )
        rows = self._read_mappings_with_guard(
            stmt,
            op_name="orders.has_client_order_key",
            fail_open=None,
        )
        return bool(rows)

    def get_order_by_client_order_key(self, env: str, client_order_key: str) -> dict | None:
        stmt = select(self._schema.orders).where(
            and_(self._schema.orders.c.env == env, self._schema.orders.c.client_order_key == client_order_key)
        )
        try:
            with self.engine.connect() as conn:
                row = conn.execute(stmt).mappings().first()
                return dict(row) if row else None
        except (OperationalError, DBAPIError, SATimeoutError, StatementError) as exc:
            logger.exception(
                "[DB][READ][FAIL] op=orders.get_order_by_client_order_key err_type=%s err=%s",
                type(exc).__name__,
                exc,
            )
            if _order_lookup_fail_open_default():
                self._last_read_fail_open_op = "orders.get_order_by_client_order_key"
                logger.warning("[DB][READ][FAIL_OPEN] op=orders.get_order_by_client_order_key -> returning None")
                return None
            raise

    def has_blocking_order_today(
        self,
        env: str,
        code: str,
        side: str,
        stage: str | None = None,
        trade_date: str | None = None,
    ) -> tuple[bool, dict | None]:
        """
        오늘 같은 종목/사이드/스테이지에 대해 '블록 상태'의 주문이 이미 존재하는지 확인.
        블록 상태: SUBMITTED, ACCEPTED, FILLED, PARTIAL_FILLED
        재시도 허용 상태: REJECTED, FAILED, ERROR, SKIP, INTENT
        
        Returns:
            (is_blocked: bool, prior_order: dict | None)
            prior_order에는 status, created_at, client_order_key, run_id 등 포함
        """
        if not trade_date:
            now = now_kst()
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
        else:
            from datetime import datetime
            day = datetime.fromisoformat(trade_date)
            start = day.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.timezone('Asia/Seoul'))
            end = start + timedelta(days=1)
        
        # 블록 상태만 필터링
        blocking_statuses = ['SUBMITTED', 'ACCEPTED', 'FILLED', 'PARTIAL_FILLED']
        
        conditions = [
            self._schema.orders.c.env == env,
            self._schema.orders.c.code == code,
            self._schema.orders.c.side == side,
            self._schema.orders.c.created_at >= start,
            self._schema.orders.c.created_at < end,
            self._schema.orders.c.status.in_(blocking_statuses),
        ]
        
        if stage:
            conditions.append(self._schema.orders.c.stage == stage)
        
        stmt = (
            select(self._schema.orders)
            .where(and_(*conditions))
            .order_by(self._schema.orders.c.created_at.desc())
            .limit(1)
        )
        fail_open = _order_lookup_fail_open_default()
        self._last_read_fail_open_op = None
        try:
            with self.engine.connect() as conn:
                row = conn.execute(stmt).mappings().first()
                if row:
                    return True, dict(row)
                return False, None
        except (OperationalError, DBAPIError, SATimeoutError, StatementError) as exc:
            logger.exception(
                "[DB][READ][FAIL] op=orders.has_blocking_order_today fail_open=%s err_type=%s err=%s",
                int(bool(fail_open)),
                type(exc).__name__,
                exc,
            )
            if fail_open:
                self._last_read_fail_open_op = "orders.has_blocking_order_today"
                logger.warning("[DB][READ][FAIL_OPEN] op=orders.has_blocking_order_today -> returning False,None")
                return False, None
            raise

    def list_today_orders(
        self,
        env: str,
        *,
        side: str | None = None,
        code: str | None = None,
        status_exclude: Iterable[str] | None = ("ERROR",),
    ) -> list[dict]:
        now = now_kst()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        start_bp = sa.bindparam("created_at_1", start, type_=sa.DateTime(timezone=True))
        end_bp = sa.bindparam("created_at_2", end, type_=sa.DateTime(timezone=True))
        conditions = [self._schema.orders.c.env == sa.bindparam("env_1", env), self._schema.orders.c.created_at >= start_bp, self._schema.orders.c.created_at < end_bp]
        if side:
            conditions.append(self._schema.orders.c.side == sa.bindparam("side_1", side))
        if code:
            conditions.append(self._schema.orders.c.code == sa.bindparam("code_1", code))
        if status_exclude:
            conditions.append(self._schema.orders.c.status.not_in(list(status_exclude)))
        stmt = select(self._schema.orders).where(and_(*conditions))
        return self._read_mappings_with_guard(
            stmt,
            op_name="orders.list_today_orders",
            fail_open=None,
        )

    def list_today_buy_orders(
        self,
        env: str,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        code: str | None = None,
        status_exclude: Iterable[str] | None = ("ERROR", "CANCELLED", "REJECTED", "FAILED", "SKIP"),
    ) -> list[dict]:
        now = now_kst()
        start = start_at or now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end_at or (start + timedelta(days=1))
        conditions = [
            self._schema.orders.c.env == sa.bindparam("env_buy_1", env),
            self._schema.orders.c.side == sa.bindparam("side_buy_1", "BUY"),
            self._schema.orders.c.created_at >= sa.bindparam("created_at_buy_1", start, type_=sa.DateTime(timezone=True)),
            self._schema.orders.c.created_at < sa.bindparam("created_at_buy_2", end, type_=sa.DateTime(timezone=True)),
        ]
        if code:
            conditions.append(self._schema.orders.c.code == sa.bindparam("code_buy_1", code))
        if status_exclude:
            conditions.append(self._schema.orders.c.status.not_in(list(status_exclude)))
        stmt = (
            select(self._schema.orders)
            .where(and_(*conditions))
            .order_by(self._schema.orders.c.created_at.desc())
        )
        return self._read_mappings_with_guard(
            stmt,
            op_name="orders.list_today_buy_orders",
            fail_open=None,
        )

    def has_today_buy_orders(
        self,
        env: str,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        code: str | None = None,
        status_exclude: Iterable[str] | None = ("ERROR", "CANCELLED", "REJECTED", "FAILED", "SKIP"),
    ) -> bool:
        return bool(
            self.list_today_buy_orders(
                env,
                start_at=start_at,
                end_at=end_at,
                code=code,
                status_exclude=status_exclude,
            )
        )

    def _session_window_bounds(self, session: str, trade_date: date | datetime | str | None = None) -> tuple[datetime, datetime]:
        session_key = str(session or "").strip().lower()
        bounds = {
            "am": (9, 0, 12, 59),
            "pm": (13, 0, 15, 39),
        }
        start_hour, start_minute, end_hour, end_minute = bounds.get(session_key, bounds["am"])
        anchor = now_kst()
        if isinstance(trade_date, datetime):
            marker_date = trade_date.date()
        elif isinstance(trade_date, date):
            marker_date = trade_date
        elif isinstance(trade_date, str) and trade_date.strip():
            marker_date = datetime.fromisoformat(trade_date).date()
        else:
            marker_date = now_kst().date()
        anchor = anchor.replace(year=marker_date.year, month=marker_date.month, day=marker_date.day)
        return (
            anchor.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0),
            anchor.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0),
        )

    def list_today_session_buy_orders(
        self,
        env: str,
        session: str,
        *,
        trade_date: date | datetime | str | None = None,
        code: str | None = None,
    ) -> list[dict]:
        start_at, end_at = self._session_window_bounds(session, trade_date)
        return self.list_today_buy_orders(
            env,
            start_at=start_at,
            end_at=end_at,
            code=code,
        )

    def has_today_session_marker(
        self,
        env: str,
        session: str,
        marker_type: str,
        trade_date: date | datetime | str | None = None,
    ) -> bool:
        marker_key = str(marker_type or "completed").strip().lower() or "completed"
        if isinstance(trade_date, datetime):
            marker_date = trade_date.date()
        elif isinstance(trade_date, date):
            marker_date = trade_date
        elif isinstance(trade_date, str) and trade_date.strip():
            marker_date = datetime.fromisoformat(trade_date).date()
        else:
            marker_date = now_kst().date()
        job_key = f"trade_session:{str(env or '').strip().lower() or 'practice'}:am:{marker_date.isoformat()}"
        if str(session or "").strip().lower() in {"am", "pm"}:
            job_key = f"trade_session:{str(env or '').strip().lower() or 'practice'}:{str(session or '').strip().lower()}:{marker_date.isoformat()}"
        schema = schema_for_engine(self.engine)
        stmt = select(schema.job_checkpoints.c.payload).where(schema.job_checkpoints.c.job_key == job_key)
        self._last_read_fail_open_op = None
        try:
            with self.engine.connect() as conn:
                payload = conn.execute(stmt).scalar()
            completed = bool((payload or {}).get(marker_key)) if isinstance(payload, dict) else False
            logger.info(
                "[DB][ORDERS][SESSION_MARKER] env=%s session=%s trade_date=%s marker_type=%s completed=%s",
                env,
                session,
                marker_date.isoformat(),
                marker_key,
                int(completed),
            )
            return completed
        except (OperationalError, DBAPIError, SATimeoutError, StatementError) as exc:
            fail_open = _order_lookup_fail_open_default()
            logger.exception(
                "[DB][READ][FAIL] op=orders.has_today_session_marker fail_open=%s err_type=%s err=%s",
                int(bool(fail_open)),
                type(exc).__name__,
                exc,
            )
            if fail_open:
                self._last_read_fail_open_op = "orders.has_today_session_marker"
                logger.warning("[DB][READ][FAIL_OPEN] op=orders.has_today_session_marker -> returning False")
                return False
            raise

    def has_today_am_session_marker(self, env: str, trade_date: date | datetime | str | None = None) -> bool:
        return self.has_today_session_marker(env, "am", "completed", trade_date)

    def list_orders_in_window(
        self,
        env: str,
        *,
        start_at: datetime,
        end_at: datetime,
        side: str | None = None,
        code: str | None = None,
        codes: Iterable[str] | None = None,
        status_include: Iterable[str] | None = None,
        status_exclude: Iterable[str] | None = None,
    ) -> list[dict]:
        env_n = _norm_env(env)
        code_list = [str(item).zfill(6) for item in (codes or []) if str(item or "").strip()]
        created_at_expr = self._window_expr(self._schema.orders.c.created_at)
        conditions = [
            self._schema.orders.c.env == env_n,
            created_at_expr >= start_at,
            created_at_expr < end_at,
        ]
        if side:
            conditions.append(self._schema.orders.c.side == str(side).upper())
        if code:
            conditions.append(self._schema.orders.c.code == str(code).zfill(6))
        elif code_list:
            conditions.append(self._schema.orders.c.code.in_(code_list))
        if status_include:
            conditions.append(self._schema.orders.c.status.in_([str(item).upper() for item in status_include]))
        if status_exclude:
            conditions.append(self._schema.orders.c.status.not_in([str(item).upper() for item in status_exclude]))
        stmt = (
            select(self._schema.orders)
            .where(and_(*conditions))
            .order_by(created_at_expr.desc())
        )
        return self._read_mappings_with_guard(
            stmt,
            op_name="orders.list_orders_in_window",
            fail_open=None,
        )

    def upsert_reconciled_order(
        self,
        *,
        env: str,
        run_id: str | None,
        strategy: str,
        sid: int,
        mode: int,
        code: str,
        market: str | None,
        side: str,
        ord_type: str,
        qty: int,
        limit_price: float | None,
        stage: str | None,
        client_order_key: str,
        kis_odno: str | None,
        status: str,
        request_json: dict | None,
        response_json: dict | None,
        submitted_at: datetime | None,
        acked_at: datetime | None,
    ) -> str:
        db_url = str(self.engine.url)
        broker_order_id = kis_odno or client_order_key
        safe_request_json = json_sanitize(request_json or {})
        safe_response_json = json_sanitize(response_json or {})
        payload = {
            "order_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": env,
            "run_id": uuid_value_for_url(db_url, run_id) if run_id is not None else None,
            "strategy": strategy,
            "sid": sid,
            "mode": mode,
            "code": code,
            "market": market,
            "side": side,
            "ord_type": ord_type,
            "qty": qty,
            "limit_price": limit_price,
            "stage": stage,
            "client_order_key": client_order_key,
            "status": status,
            "kis_odno": kis_odno,
            "broker_order_id": broker_order_id,
            "request_json": safe_request_json,
            "response_json": safe_response_json,
            "submitted_at": submitted_at,
            "acked_at": acked_at,
        }
        conflict_cols = ["env", "broker_order_id"] if broker_order_id else ["env", "client_order_key"]
        update_cols = {
            "status": status,
            "kis_odno": kis_odno,
            "broker_order_id": broker_order_id,
            "response_json": safe_response_json,
            "request_json": safe_request_json,
            "submitted_at": submitted_at,
            "acked_at": acked_at,
            "updated_at": func.now(),
        }
        with self.engine.begin() as conn:
            if conn.dialect.name == "postgresql":
                # Use PostgreSQL-specific upsert with on_conflict_do_update
                stmt = pg_insert(self._schema.orders).values(**payload).on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_=update_cols,
                ).returning(self._schema.orders.c.order_id)
                res = conn.execute(stmt)
                order_id = res.scalar()
                if order_id:
                    return str(order_id)
            else:
                # Fallback upsert for non-PostgreSQL dialects
                try:
                    stmt = sa.insert(self._schema.orders).values(**payload).returning(self._schema.orders.c.order_id)
                    res = conn.execute(stmt)
                    order_id = res.scalar()
                    if order_id:
                        return str(order_id)
                except IntegrityError:
                    # Update existing row
                    where_conditions = [self._schema.orders.c[col] == payload[col] for col in conflict_cols]
                    conn.execute(
                        sa.update(self._schema.orders).where(and_(*where_conditions)).values(**update_cols)
                    )
                    existing = conn.execute(
                        select(self._schema.orders.c.order_id).where(and_(*where_conditions))
                    ).scalar()
                    if existing:
                        return str(existing)
            
            # Fallback query if stmt didn't return anything
            existing = conn.execute(
                select(self._schema.orders.c.order_id).where(
                    and_(
                        self._schema.orders.c.env == env,
                        self._schema.orders.c.client_order_key == client_order_key,
                    )
                )
            ).scalar()
            return str(existing or payload["order_id"])


class FillsRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)
        self._last_read_fail_open_op: str | None = None

    def _window_expr(self, column):
        if self.engine.dialect.name == "postgresql":
            return sa.cast(column, sa.DateTime(timezone=True))
        return column

    def _filled_at_window_conditions(self, *, env: str, start_at: datetime, end_at: datetime) -> list[Any]:
        filled_at_expr = self._window_expr(self._schema.fills.c.filled_at)
        return [
            self._schema.fills.c.env == _norm_env(env),
            filled_at_expr >= bindparam("filled_at_start", start_at, type_=sa.DateTime(timezone=True)),
            filled_at_expr < bindparam("filled_at_end", end_at, type_=sa.DateTime(timezone=True)),
        ]

    def _is_filled_at_schema_mismatch(self, exc: Exception) -> bool:
        message = str(getattr(exc, "orig", exc) or exc).lower()
        return (
            "operator does not exist" in message
            and "text >= timestamp with time zone" in message
        )

    def consume_fail_open_marker(self, op_name: str) -> bool:
        if self._last_read_fail_open_op != op_name:
            return False
        self._last_read_fail_open_op = None
        return True

    def _read_mappings_with_guard(
        self,
        stmt,
        *,
        op_name: str,
        fail_open: bool | None = None,
    ) -> list[dict]:
        if fail_open is None:
            fail_open = _order_lookup_fail_open_default()
        self._last_read_fail_open_op = None
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(stmt).mappings().all()
                logger.info("[DB][READ][OK] op=%s rows=%s fail_open=%s", op_name, len(rows), int(bool(fail_open)))
                return [dict(r) for r in rows]
        except (OperationalError, DBAPIError, SATimeoutError, StatementError) as exc:
            logger.exception(
                "[DB][READ][FAIL] op=%s fail_open=%s err_type=%s err=%s",
                op_name,
                int(bool(fail_open)),
                type(exc).__name__,
                exc,
            )
            if fail_open:
                self._last_read_fail_open_op = op_name
                logger.warning("[DB][READ][FAIL_OPEN] op=%s -> returning []", op_name)
                return []
            raise

    def ensure_run_exists(self, run_id: str) -> None:
        # Check if run exists, if not, insert minimal row
        stmt = sa.select(self._schema.runs.c.run_id).where(self._schema.runs.c.run_id == run_id)
        with self.engine.begin() as conn:
            exists = conn.execute(stmt).first() is not None
            if not exists:
                # Insert minimal row
                values = {
                    "run_id": _coerce_uuid(run_id, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                    "env": "unknown",
                    "strategy": "unknown",
                    "status": "STARTED",
                }
                conn.execute(sa.insert(self._schema.runs).values(**values))

    def list_today_fills(
        self,
        env: str,
        *,
        side: str | None = None,
        code: str | None = None,
    ) -> list[dict]:
        now = now_kst()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        filled_at_expr = self._window_expr(self._schema.fills.c.filled_at)
        conditions = self._filled_at_window_conditions(env=env, start_at=start, end_at=end)
        if side:
            conditions.append(self._schema.fills.c.side == str(side).upper())
        if code:
            conditions.append(self._schema.fills.c.code == str(code).zfill(6))
        stmt = select(self._schema.fills).where(and_(*conditions)).order_by(filled_at_expr.desc())
        fail_open = _order_lookup_fail_open_default()
        try:
            return self._read_mappings_with_guard(
                stmt,
                op_name="fills.list_today_fills",
                fail_open=fail_open,
            )
        except ProgrammingError as exc:
            logger.exception(
                "[DB][READ][FAIL] op=%s fail_open=%s err_type=%s err=%s",
                "fills.list_today_fills",
                int(bool(fail_open)),
                type(exc).__name__,
                exc,
            )
            if self._is_filled_at_schema_mismatch(exc):
                logger.error(
                    "[DB][READ][FATAL_SCHEMA_MISMATCH] op=fills.list_today_fills column=filled_at expected=timestamptz"
                )
                raise RuntimeError(
                    "ENTRY_ABORT_PRECHECK:db_schema_mismatch_fills_filled_at op=fills.list_today_fills [DB][READ][FATAL_SCHEMA_MISMATCH]"
                ) from exc
            raise

    def list_fills_in_window(
        self,
        env: str,
        *,
        start_at: datetime,
        end_at: datetime,
        side: str | None = None,
        code: str | None = None,
        codes: Iterable[str] | None = None,
    ) -> list[dict]:
        env_n = _norm_env(env)
        code_list = [str(item).zfill(6) for item in (codes or []) if str(item or "").strip()]
        filled_at_expr = self._window_expr(self._schema.fills.c.filled_at)
        conditions = self._filled_at_window_conditions(env=env_n, start_at=start_at, end_at=end_at)
        if side:
            conditions.append(self._schema.fills.c.side == str(side).upper())
        if code:
            conditions.append(self._schema.fills.c.code == str(code).zfill(6))
        elif code_list:
            conditions.append(self._schema.fills.c.code.in_(code_list))
        stmt = select(self._schema.fills).where(and_(*conditions)).order_by(filled_at_expr.desc())
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def list_latest_buy_fills_by_codes(self, env: str, codes: Iterable[str]) -> dict[str, dict]:
        normalized_codes = [str(code or "").zfill(6) for code in (codes or []) if str(code or "").strip()]
        if not normalized_codes:
            return {}
        stmt = (
            select(self._schema.fills)
            .where(
                and_(
                    self._schema.fills.c.env == _norm_env(env),
                    self._schema.fills.c.side == "BUY",
                    self._schema.fills.c.code.in_(normalized_codes),
                )
            )
            .order_by(self._window_expr(self._schema.fills.c.filled_at).desc())
        )
        latest: dict[str, dict] = {}
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        for row in rows:
            item = dict(row)
            code = str(item.get("code") or "").zfill(6)
            if code and code not in latest:
                latest[code] = item
        return latest

    def upsert_fill(
        self,
        *,
        env: str,
        run_id: str | None,
        order_id: str | None,
        kis_odno: str | None,
        trade_id: str | None,
        code: str,
        market: str | None,
        side: str,
        qty: int,
        price: float,
        fee: float,
        tax: float,
        filled_at: datetime,
        raw_json: dict | None,
        fill_meta_json: dict | None = None,
    ) -> str:
        db_url = str(self.engine.url)
        broker_fill_id = trade_id or None
        safe_raw_json = json_sanitize(raw_json or {})
        if run_id:
            self.ensure_run_exists(run_id)
        payload = {
            "fill_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": env,
            "run_id": uuid_value_for_url(db_url, run_id) if run_id is not None else None,
            "order_id": uuid_value_for_url(db_url, order_id) if order_id is not None else None,
            "kis_odno": kis_odno,
            "trade_id": trade_id,
            "broker_fill_id": broker_fill_id,
            "code": code,
            "market": market,
            "side": side,
            "qty": qty,
            "price": price,
            "fee": fee,
            "tax": tax,
            "filled_at": filled_at,
            "raw_json": safe_raw_json,
            **_entry_meta_columns(fill_meta_json, json_field="fill_meta_json"),
        }
        conflict_cols = ["env", "broker_fill_id"] if broker_fill_id else [
            "env",
            "kis_odno",
            "code",
            "side",
            "qty",
            "price",
            "filled_at",
        ]
        update_cols = {
            "raw_json": safe_raw_json,
            "broker_fill_id": broker_fill_id,
            **_entry_meta_columns(fill_meta_json, json_field="fill_meta_json"),
        }
        with self.engine.begin() as conn:
            if conn.dialect.name == "postgresql":
                # Use PostgreSQL-specific upsert with on_conflict_do_update
                stmt = pg_insert(self._schema.fills).values(**payload).on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_=update_cols,
                ).returning(self._schema.fills.c.fill_id)
                res = conn.execute(stmt)
                fill_id = res.scalar()
                if fill_id:
                    return str(fill_id)
            else:
                # Fallback upsert for non-PostgreSQL dialects
                try:
                    stmt = sa.insert(self._schema.fills).values(**payload).returning(self._schema.fills.c.fill_id)
                    res = conn.execute(stmt)
                    fill_id = res.scalar()
                    if fill_id:
                        return str(fill_id)
                except IntegrityError:
                    # Update existing row
                    where_conditions = [self._schema.fills.c[col] == payload[col] for col in conflict_cols]
                    conn.execute(
                        sa.update(self._schema.fills).where(and_(*where_conditions)).values(**update_cols)
                    )
                    existing = conn.execute(
                        select(self._schema.fills.c.fill_id).where(and_(*where_conditions))
                    ).scalar()
                    if existing:
                        return str(existing)
            
            # Fallback query if stmt didn't return anything
            existing = None
            if trade_id:
                existing = conn.execute(
                    select(self._schema.fills.c.fill_id).where(
                        and_(self._schema.fills.c.env == env, self._schema.fills.c.trade_id == trade_id)
                    )
                ).scalar()
            return str(existing or payload["fill_id"])


class LedgerEventsRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def _window_expr(self, column):
        if self.engine.dialect.name == "postgresql":
            return sa.cast(column, sa.DateTime(timezone=True))
        return column

    def _payload_as_of_expr(self) -> sa.sql.ClauseElement:
        if self.engine.dialect.name == "postgresql":
            return self._schema.ledger_events.c.payload_json["as_of"].astext
        return func.json_extract(self._schema.ledger_events.c.payload_json, "$.as_of")

    def _payload_text_expr(self, key: str) -> sa.sql.ClauseElement:
        if self.engine.dialect.name == "postgresql":
            return self._schema.ledger_events.c.payload_json[str(key)].astext
        return func.json_extract(self._schema.ledger_events.c.payload_json, f"$.{key}")

    def ensure_run_exists(self, run_id: str) -> None:
        # Check if run exists, if not, insert minimal row
        stmt = sa.select(self._schema.runs.c.run_id).where(self._schema.runs.c.run_id == run_id)
        with self.engine.begin() as conn:
            exists = conn.execute(stmt).first() is not None
            if not exists:
                # Insert minimal row
                values = {
                    "run_id": _coerce_uuid(run_id, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                    "env": "unknown",
                    "strategy": "unknown",
                    "status": "STARTED",
                }
                conn.execute(sa.insert(self._schema.runs).values(**values))

    def append_event(
        self,
        *,
        env: str,
        run_id: str | None,
        strategy: str | None = None,
        run_window: str | None = None,
        event_type: str,
        ts: datetime,
        code: str | None = None,
        market: str | None = None,
        sid: int | None = None,
        mode: int | None = None,
        side: str | None = None,
        qty: int | None = None,
        price: float | None = None,
        kis_odno: str | None = None,
        client_order_key: str | None = None,
        ok: bool = True,
        reasons: list[str] | None = None,
        stage: str | None = None,
        payload_json: dict | None = None,
    ) -> str | None:
        db_url = str(self.engine.url)
        db_store_required = os.getenv("DB_STORE_REQUIRED", "0") not in {"0", "false", "FALSE"}
        max_attempts = int(os.getenv("DB_WRITE_MAX_ATTEMPTS", "2"))
        base_backoff = float(os.getenv("DB_WRITE_BACKOFF_SEC", "0.2"))
        safe_payload_json = json_sanitize(payload_json) if payload_json is not None else {}
        safe_reasons = json_sanitize(reasons or [])
        if run_id:
            self.ensure_run_exists(run_id)
        payload = {
            "ledger_event_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": env,
            "run_id": uuid_value_for_url(db_url, run_id) if run_id is not None else None,
            "event_type": event_type,
            "ts": ts,
            "code": code,
            "market": market,
            "sid": sid,
            "mode": mode,
            "side": side,
            "qty": qty,
            "price": price,
            "kis_odno": kis_odno,
            "client_order_key": client_order_key,
            "ok": ok,
            "reasons": safe_reasons,
            "stage": stage,
            "payload_json": safe_payload_json,
        }
        stmt = sa.insert(self._schema.ledger_events).values(**payload).returning(self._schema.ledger_events.c.ledger_event_id)
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                with self.engine.begin() as conn:
                    logger.info(
                        "[DB][LEDGER_EVENT][APPEND] attempt=%s env=%s run_id=%s strategy=%s event_type=%s",
                        attempt,
                        env,
                        run_id,
                        strategy,
                        event_type,
                    )
                    if run_id is not None and strategy:
                        ensure_run(
                            conn,
                            self._schema,
                            run_id=run_id,
                            env=env,
                            run_window=run_window,
                            strategy=strategy,
                            ts=ts,
                            database_url=db_url,
                        )
                    res = conn.execute(stmt)
                    return str(res.scalar())
            except Exception as exc:
                last_exc = exc
                # FK 위반 시 runs 테이블 upsert 재시도
                is_fk_violation = (
                    isinstance(exc, IntegrityError) and 
                    "foreign key" in str(exc).lower() and 
                    "run_id" in str(exc).lower()
                )
                if is_fk_violation and attempt == 1:
                    logger.warning(
                        "[DB][LEDGER_EVENT][FK-VIOLATION] run_id=%s not in runs table, will ensure_run and retry",
                        run_id,
                    )
                    try:
                        with self.engine.begin() as conn:
                            ensure_run(
                                conn,
                                self._schema,
                                run_id=run_id,
                                env=env,
                                run_window=run_window,
                                strategy=strategy or "unknown",
                                ts=ts,
                                database_url=db_url,
                            )
                        continue  # retry
                    except Exception as ensure_exc:
                        logger.exception("[DB][LEDGER_EVENT][ENSURE_RUN_FAIL] err=%s", ensure_exc)
                if attempt == 1:
                    logger.exception(
                        "[DB][LEDGER_EVENT][FAIL-FIRST] attempt=%s env=%s run_id=%s strategy=%s event_type=%s sql=%s payload=%s",
                        attempt,
                        env,
                        run_id,
                        strategy,
                        event_type,
                        stmt,
                        payload,
                    )
                if _is_in_failed_transaction_error(exc):
                    if attempt > 1:
                        logger.warning(
                            "[DB][LEDGER_EVENT][RETRY-IFT] attempt=%s err_type=%s err=%s",
                            attempt,
                            type(exc).__name__,
                            exc,
                        )
                        logger.info(
                            "[DB][LEDGER_EVENT][RETRY] reason=in_failed_transaction attempt=%s env=%s run_id=%s strategy=%s event_type=%s",
                            attempt,
                            env,
                            run_id,
                            strategy,
                            event_type,
                        )
                    try:
                        self.engine.dispose()
                    except Exception as dispose_exc:
                        logger.warning(
                            "[DB][LEDGER_EVENT][DISPOSE-FAIL] err_type=%s err=%s",
                            type(dispose_exc).__name__,
                            dispose_exc,
                        )
                elif attempt > 1:
                    logger.exception(
                        "[DB][LEDGER_EVENT][FAIL] attempt=%s env=%s run_id=%s strategy=%s event_type=%s err=%s sql=%s payload=%s",
                        attempt,
                        env,
                        run_id,
                        strategy,
                        event_type,
                        exc,
                        stmt,
                        payload,
                    )
                if attempt < max_attempts:
                    time.sleep(base_backoff * (2 ** (attempt - 1)))
        if db_store_required and last_exc is not None:
            raise last_exc
        if last_exc is not None:
            logger.warning(
                "[DB][LEDGER_EVENT][LAST_ERROR] err_type=%s err=%s",
                type(last_exc).__name__,
                last_exc,
            )
        logger.warning(
            "[DB][LEDGER_EVENT][SKIP] env=%s run_id=%s strategy=%s event_type=%s required=%s attempts=%s",
            env,
            run_id,
            strategy,
            event_type,
            int(db_store_required),
            max_attempts,
        )
        return None

    def append_event_from_context(
        self,
        ctx: RunContext,
        event_type: str,
        ts: datetime,
        code: str | None = None,
        market: str | None = None,
        sid: int | None = None,
        mode: int | None = None,
        side: str | None = None,
        qty: int | None = None,
        price: float | None = None,
        kis_odno: str | None = None,
        client_order_key: str | None = None,
        ok: bool = True,
        reasons: list[str] | None = None,
        stage: str | None = None,
        payload_json: dict | None = None,
    ) -> str | None:
        assert_uuid(ctx.run_id)
        db_url = str(self.engine.url)
        db_store_required = os.getenv("DB_STORE_REQUIRED", "0") not in {"0", "false", "FALSE"}
        max_attempts = int(os.getenv("DB_WRITE_MAX_ATTEMPTS", "2"))
        base_backoff = float(os.getenv("DB_WRITE_BACKOFF_SEC", "0.2"))
        safe_payload_json = json_sanitize(payload_json) if payload_json is not None else {}
        safe_reasons = json_sanitize(reasons or [])
        payload = {
            "ledger_event_id": _coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=db_url),
            "env": ctx.env,
            "run_id": uuid_value_for_url(db_url, ctx.run_id),
            "event_type": event_type,
            "ts": ts,
            "code": code,
            "market": market,
            "sid": sid,
            "mode": mode,
            "side": side,
            "qty": qty,
            "price": price,
            "kis_odno": kis_odno,
            "client_order_key": client_order_key,
            "ok": ok,
            "reasons": safe_reasons,
            "stage": stage,
            "payload_json": safe_payload_json,
        }
        stmt = sa.insert(self._schema.ledger_events).values(**payload).returning(self._schema.ledger_events.c.ledger_event_id)
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                with self.engine.begin() as conn:
                    ensure_run_from_context(conn, self._schema, ctx, db_url)
                    logger.info(
                        "[DB][LEDGER_EVENT][APPEND] attempt=%s env=%s run_id=%s strategy=%s event_type=%s",
                        attempt,
                        ctx.env,
                        ctx.run_id,
                        ctx.strategy,
                        event_type,
                    )
                    res = conn.execute(stmt)
                    return str(res.scalar())
            except Exception as exc:
                last_exc = exc
                if attempt == 1:
                    logger.exception(
                        "[DB][LEDGER_EVENT][FAIL-FIRST] attempt=%s env=%s run_id=%s strategy=%s event_type=%s sql=%s payload=%s",
                        attempt,
                        ctx.env,
                        ctx.run_id,
                        ctx.strategy,
                        event_type,
                        stmt,
                        payload,
                    )
                if _is_in_failed_transaction_error(exc):
                    if attempt > 1:
                        logger.warning(
                            "[DB][LEDGER_EVENT][RETRY-IFT] attempt=%s err_type=%s err=%s",
                            attempt,
                            type(exc).__name__,
                            exc,
                        )
                        logger.info(
                            "[DB][LEDGER_EVENT][RETRY] reason=in_failed_transaction attempt=%s env=%s run_id=%s strategy=%s event_type=%s",
                            attempt,
                            ctx.env,
                            ctx.run_id,
                            ctx.strategy,
                            event_type,
                        )
                    try:
                        self.engine.dispose()
                    except Exception as dispose_exc:
                        logger.warning(
                            "[DB][LEDGER_EVENT][DISPOSE-FAIL] err_type=%s err=%s",
                            type(dispose_exc).__name__,
                            dispose_exc,
                        )
                elif attempt > 1:
                    logger.exception(
                        "[DB][LEDGER_EVENT][FAIL] attempt=%s env=%s run_id=%s strategy=%s event_type=%s err=%s sql=%s payload=%s",
                        attempt,
                        ctx.env,
                        ctx.run_id,
                        ctx.strategy,
                        event_type,
                        exc,
                        stmt,
                        payload,
                    )
                if attempt < max_attempts:
                    time.sleep(base_backoff * (2 ** (attempt - 1)))
        if db_store_required and last_exc is not None:
            raise last_exc
        if last_exc is not None:
            logger.warning(
                "[DB][LEDGER_EVENT][LAST_ERROR] err_type=%s err=%s",
                type(last_exc).__name__,
                last_exc,
            )
        logger.warning(
            "[DB][LEDGER_EVENT][SKIP] env=%s run_id=%s strategy=%s event_type=%s required=%s attempts=%s",
            ctx.env,
            ctx.run_id,
            ctx.strategy,
            event_type,
            int(db_store_required),
            max_attempts,
        )
        return None

    def upsert_prep_event(
        self,
        *,
        env: str,
        strategy: str = "pb1",
        as_of=None,
        trade_date=None,
        event_type: str = "PREP_DONE",
        status: str,
        reason: str,
        final30_count: int,
        quality_ok: bool,
        trade_can_proceed: bool,
        run_id: str | None = None,
        run_window: str | None = "prep",
    ) -> str | None:
        env_n = _norm_env(env)
        strategy_n = _norm_strategy(strategy) or "pb1"
        as_of_str = str(as_of)
        trade_date_str = str(trade_date)
        event_type_n = str(event_type or "PREP_DONE").strip().upper() or "PREP_DONE"
        status_s = str(status or "").strip()
        reason_s = str(reason or "").strip()
        payload = {
            "env": env_n,
            "strategy": strategy_n,
            "as_of": as_of_str,
            "trade_date": trade_date_str,
            "event_type": event_type_n,
            "status": status_s,
            "reason": reason_s,
            "final30_count": int(final30_count or 0),
            "quality_ok": int(bool(quality_ok)),
            "trade_can_proceed": int(bool(trade_can_proceed)),
        }
        stage = "prep_done_check" if reason_s == "ledger_missing_but_final30_canonical_ok" else "prep_duplicate_guard"

        try:
            stmt = (
                sa.select(
                    self._schema.ledger_events.c.ledger_event_id,
                    self._schema.ledger_events.c.payload_json,
                )
                .where(self._schema.ledger_events.c.env == env_n)
                .where(self._schema.ledger_events.c.event_type == event_type_n)
                .where(self._payload_as_of_expr() == as_of_str)
                .order_by(self._schema.ledger_events.c.ts.desc())
                .limit(20)
            )
            with self.engine.connect() as conn:
                rows = conn.execute(stmt).mappings().all()

            for row in rows:
                payload_json = dict(row.get("payload_json") or {})
                if (
                    str(payload_json.get("as_of")) == as_of_str
                    and str(payload_json.get("trade_date")) == trade_date_str
                    and _norm_strategy(payload_json.get("strategy")) == strategy_n
                    and str(payload_json.get("status") or "").strip() == status_s
                    and str(payload_json.get("reason") or "").strip() == reason_s
                ):
                    event_id = str(row.get("ledger_event_id"))
                    logger.info(
                        "[DB][LEDGER][PREP_EVENT][UPSERT_SKIP_EXISTS] env=%s strategy=%s as_of=%s trade_date=%s event=%s status=%s reason=%s event_id=%s",
                        env_n,
                        strategy_n,
                        as_of_str,
                        trade_date_str,
                        event_type_n,
                        status_s,
                        reason_s,
                        event_id,
                    )
                    return event_id
        except Exception as exc:
            logger.warning(
                "[DB][LEDGER][PREP_EVENT][EXISTS_CHECK_FAIL] env=%s strategy=%s as_of=%s trade_date=%s err_type=%s err=%s",
                env_n,
                strategy_n,
                as_of_str,
                trade_date_str,
                type(exc).__name__,
                exc,
            )

        try:
            event_id = self.append_event(
                env=env_n,
                run_id=run_id,
                strategy=strategy_n,
                run_window=run_window,
                event_type=event_type_n,
                ts=now_kst(),
                ok=bool(quality_ok and trade_can_proceed),
                reasons=[reason_s] if reason_s else None,
                stage=stage,
                payload_json=payload,
            )
            logger.info(
                "[DB][LEDGER][PREP_EVENT][UPSERT_OK] env=%s strategy=%s as_of=%s trade_date=%s event=%s status=%s reason=%s event_id=%s",
                env_n,
                strategy_n,
                as_of_str,
                trade_date_str,
                event_type_n,
                status_s,
                reason_s,
                event_id,
            )
            return event_id
        except Exception as exc:
            db_store_required = str(os.getenv("DB_STORE_REQUIRED", "0")).lower() not in {"0", "false", "no", "off"}
            logger.exception(
                "[DB][LEDGER][PREP_EVENT][UPSERT_FAIL] env=%s strategy=%s as_of=%s trade_date=%s event=%s status=%s reason=%s required=%s err_type=%s err=%s",
                env_n,
                strategy_n,
                as_of_str,
                trade_date_str,
                event_type_n,
                status_s,
                reason_s,
                int(db_store_required),
                type(exc).__name__,
                exc,
            )
            if db_store_required:
                raise
            return None

    def get_prep_event(
        self,
        *,
        env: str,
        strategy: str,
        as_of: date | str,
        trade_date: date | str,
        event_type: str = "PREP_DONE",
    ) -> dict[str, Any] | None:
        env_n = _norm_env(env)
        strategy_n = _norm_strategy(strategy)
        as_of_str = str(as_of)
        trade_date_str = str(trade_date)
        stmt = (
            sa.select(self._schema.ledger_events)
            .where(self._schema.ledger_events.c.env == env_n)
            .where(self._schema.ledger_events.c.event_type == str(event_type or "PREP_DONE").strip().upper())
            .where(self._payload_as_of_expr() == as_of_str)
            .order_by(self._schema.ledger_events.c.ts.desc())
            .limit(20)
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()
        for row in rows:
            payload_json = dict(row.get("payload_json") or {})
            if (
                _norm_strategy(payload_json.get("strategy")) == strategy_n
                and str(payload_json.get("trade_date")) == trade_date_str
            ):
                return dict(row)
        return None

    def prep_done_status(
        self,
        *,
        env: str,
        as_of: date | str,
        strategies: Iterable[str] | None = None,
        trade_date: date | str | None = None,
    ) -> tuple[bool, int]:
        schema = self._schema
        as_of_date = to_date(as_of)
        as_of_str = as_of_date.isoformat()
        payload_as_of = self._payload_as_of_expr()
        payload_strategy = func.lower(self._payload_text_expr("strategy"))
        payload_trade_date = self._payload_text_expr("trade_date")
        as_of_match = or_(
            func.date(schema.ledger_events.c.ts) == as_of_date,
            payload_as_of == as_of_str,
        )

        stmt = select(func.count()).select_from(schema.ledger_events)
        if strategies:
            strategies_norm = [s.strip().lower() for s in strategies if s and str(s).strip()]
            if strategies_norm:
                stmt = stmt.select_from(
                    schema.ledger_events.outerjoin(schema.runs, schema.runs.c.run_id == schema.ledger_events.c.run_id)
                ).where(
                    or_(
                        func.lower(schema.runs.c.strategy).in_(strategies_norm),
                        payload_strategy.in_(strategies_norm),
                    )
                )

        conditions = [
            schema.ledger_events.c.env == _norm_env(env),
            schema.ledger_events.c.event_type == "PREP_DONE",
            as_of_match,
        ]
        if trade_date is not None:
            trade_date_date = to_date(trade_date)
            conditions.append(
                or_(
                    payload_trade_date == trade_date_date.isoformat(),
                    func.date(schema.ledger_events.c.ts) == trade_date_date,
                )
            )

        stmt = stmt.where(and_(*conditions))
        with self.engine.connect() as conn:
            count = conn.execute(stmt).scalar() or 0
        return int(count) > 0, int(count)

    def has_event_type_on_date(self, *, env: str, event_type: str, as_of: date | str) -> bool:
        schema = self._schema
        as_of_date = to_date(as_of)
        stmt = select(func.count()).select_from(schema.ledger_events).where(
            and_(
                schema.ledger_events.c.env == env,
                schema.ledger_events.c.event_type == event_type,
                func.date(schema.ledger_events.c.ts) == as_of_date,
            )
        )
        with self.engine.connect() as conn:
            count = conn.execute(stmt).scalar() or 0
        return int(count) > 0

    def list_events_in_window(
        self,
        env: str,
        *,
        start_at: datetime,
        end_at: datetime,
        code: str | None = None,
        codes: Iterable[str] | None = None,
        event_types: Iterable[str] | None = None,
        side: str | None = None,
    ) -> list[dict]:
        env_n = _norm_env(env)
        code_list = [str(item).zfill(6) for item in (codes or []) if str(item or "").strip()]
        ts_expr = self._window_expr(self._schema.ledger_events.c.ts)
        conditions = [
            self._schema.ledger_events.c.env == env_n,
            ts_expr >= start_at,
            ts_expr < end_at,
        ]
        if code:
            conditions.append(self._schema.ledger_events.c.code == str(code).zfill(6))
        elif code_list:
            conditions.append(self._schema.ledger_events.c.code.in_(code_list))
        if event_types:
            conditions.append(self._schema.ledger_events.c.event_type.in_([str(item).upper() for item in event_types]))
        if side:
            conditions.append(self._schema.ledger_events.c.side == str(side).upper())
        stmt = (
            select(self._schema.ledger_events)
            .where(and_(*conditions))
            .order_by(ts_expr.desc())
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]


class PositionsRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def _get_position_row(self, env: str, strategy: str, sid: int, mode: int, code: str) -> Optional[dict]:
        stmt = select(self._schema.positions).where(
            and_(
                self._schema.positions.c.env == env,
                self._schema.positions.c.strategy == strategy,
                self._schema.positions.c.sid == sid,
                self._schema.positions.c.mode == mode,
                self._schema.positions.c.code == code,
            )
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
            return dict(row) if row else None

    def list_positions(self, env: str, strategy: str) -> list[dict]:
        stmt = select(self._schema.positions).where(
            and_(self._schema.positions.c.env == env, self._schema.positions.c.strategy == strategy)
        )
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(stmt).mappings().all()
                return [dict(r) for r in rows]
        except (OperationalError, DBAPIError, SATimeoutError, StatementError) as exc:
            logger.exception(
                "[DB][READ][FAIL] op=positions.list_positions err_type=%s err=%s",
                type(exc).__name__,
                exc,
            )
            if _practice_env_default_fail_open():
                logger.warning("[DB][READ][FAIL_OPEN] op=positions.list_positions -> returning []")
                return []
            raise

    def get_position(self, *, env: str, strategy: str, sid: int, mode: int, code: str) -> dict | None:
        stmt = select(self._schema.positions).where(
            and_(
                self._schema.positions.c.env == env,
                self._schema.positions.c.strategy == strategy,
                self._schema.positions.c.sid == sid,
                self._schema.positions.c.mode == mode,
                self._schema.positions.c.code == code,
            )
        )
        with self.engine.begin() as conn:
            row = conn.execute(stmt).mappings().first()
            return dict(row) if row else None

    def list_positions_by_codes(self, *, env: str, strategy: str, codes: list[str]) -> list[dict]:
        if not codes:
            return []
        stmt = select(self._schema.positions).where(
            and_(
                self._schema.positions.c.env == env,
                self._schema.positions.c.strategy == strategy,
                self._schema.positions.c.code.in_(codes),
            )
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
            return [dict(r) for r in rows]

    def close_positions(self, *, env: str, strategy: str, codes: list[str]) -> int:
        if not codes:
            return 0
        stmt = (
            sa.update(self._schema.positions)
            .where(
                and_(
                    self._schema.positions.c.env == env,
                    self._schema.positions.c.strategy == strategy,
                    self._schema.positions.c.code.in_(codes),
                    self._schema.positions.c.qty > 0,
                )
            )
            .values(qty=0, avg_buy_price=None, total_cost=0.0, updated_at=func.now())
        )
        with self.engine.begin() as conn:
            result = conn.execute(stmt)
            return int(result.rowcount or 0)

    def update_position_fields(
        self,
        *,
        env: str,
        strategy: str,
        sid: int,
        mode: int,
        code: str,
        fields: dict,
    ) -> None:
        if not fields:
            return
        values = dict(fields)
        fail_soft = set(values).isdisjoint({"qty", "avg_buy_price", "total_cost", "realized_pnl"})
        try:
            with self.engine.begin() as conn:
                existing = conn.execute(
                    select(self._schema.positions).where(
                        and_(
                            self._schema.positions.c.env == env,
                            self._schema.positions.c.strategy == strategy,
                            self._schema.positions.c.sid == sid,
                            self._schema.positions.c.mode == mode,
                            self._schema.positions.c.code == code,
                        )
                    )
                ).mappings().first()
                existing_row = dict(existing) if existing else {}
                if "entry_meta_json" in values:
                    values["entry_meta_json"] = _merge_json_dict(existing_row.get("entry_meta_json"), values.get("entry_meta_json"))
                if "last_exit_eval_json" in values:
                    values["last_exit_eval_json"] = _merge_json_dict(existing_row.get("last_exit_eval_json"), values.get("last_exit_eval_json"))
                stmt = (
                    sa.update(self._schema.positions)
                    .where(
                        and_(
                            self._schema.positions.c.env == env,
                            self._schema.positions.c.strategy == strategy,
                            self._schema.positions.c.sid == sid,
                            self._schema.positions.c.mode == mode,
                            self._schema.positions.c.code == code,
                        )
                    )
                    .values(**values, updated_at=func.now())
                )
                conn.execute(stmt)
        except Exception as exc:
            if fail_soft:
                logger.warning(
                    "[POSITIONS][UPDATE][FAIL_SOFT] env=%s strategy=%s code=%s fields=%s err=%s",
                    env,
                    strategy,
                    code,
                    sorted(values.keys()),
                    exc,
                )
                return
            raise

    def apply_fill(
        self,
        *,
        env: str,
        strategy: str,
        sid: int,
        mode: int,
        code: str,
        market: str | None,
        side: str,
        qty: int,
        price: float,
        fee: float,
        tax: float,
        filled_at: datetime,
        entry_meta_json: dict | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            stmt = select(self._schema.positions).where(
                and_(
                    self._schema.positions.c.env == env,
                    self._schema.positions.c.strategy == strategy,
                    self._schema.positions.c.sid == sid,
                    self._schema.positions.c.mode == mode,
                    self._schema.positions.c.code == code,
                )
            )
            row = conn.execute(stmt).mappings().first()
            if row:
                row = dict(row)
            qty = int(qty)
            cost_delta = (qty * float(price)) + float(fee) + float(tax)
            if row:
                current_qty = int(row.get("qty") or 0)
                avg_buy_price = float(row.get("avg_buy_price") or 0.0)
                total_cost = float(row.get("total_cost") or 0.0)
                realized_pnl = float(row.get("realized_pnl") or 0.0)
            else:
                current_qty = 0
                avg_buy_price = 0.0
                total_cost = 0.0
                realized_pnl = 0.0

            if side.upper() == "BUY":
                new_qty = current_qty + qty
                new_total_cost = total_cost + cost_delta
                new_avg = new_total_cost / new_qty if new_qty > 0 else None
                values = {
                    "qty": new_qty,
                    "avg_buy_price": new_avg,
                    "total_cost": new_total_cost,
                    "realized_pnl": realized_pnl,
                    "market": market,
                    "last_trade_at": filled_at,
                }
                if entry_meta_json:
                    merged_entry_meta = _merge_json_dict(row.get("entry_meta_json") if row else None, entry_meta_json)
                    values.update(
                        {
                            "entry_reason": merged_entry_meta.get("entry_reason") or row.get("entry_reason") if row else merged_entry_meta.get("entry_reason"),
                            "entry_style_selected": merged_entry_meta.get("entry_style_selected") or row.get("entry_style_selected") if row else merged_entry_meta.get("entry_style_selected"),
                            "entry_decision_family": merged_entry_meta.get("entry_decision_family") or row.get("entry_decision_family") if row else merged_entry_meta.get("entry_decision_family"),
                            "entry_rule_version": merged_entry_meta.get("entry_rule_version") or row.get("entry_rule_version") if row else merged_entry_meta.get("entry_rule_version"),
                            "entry_meta_json": merged_entry_meta,
                            "stop_price_at_entry": _safe_float_or_none(merged_entry_meta.get("stop_price_at_entry")) or row.get("stop_price_at_entry") if row else _safe_float_or_none(merged_entry_meta.get("stop_price_at_entry")),
                            "pivot_price_at_entry": _safe_float_or_none(merged_entry_meta.get("pivot_price_at_entry")) or row.get("pivot_price_at_entry") if row else _safe_float_or_none(merged_entry_meta.get("pivot_price_at_entry")),
                            "exit_policy_family": merged_entry_meta.get("exit_policy_family") or row.get("exit_policy_family") if row else merged_entry_meta.get("exit_policy_family"),
                        }
                    )
            else:
                qty_to_close = min(qty, current_qty)
                remaining_qty = max(current_qty - qty_to_close, 0)
                proceeds = (float(price) * qty_to_close) - float(fee) - float(tax)
                cost_basis = avg_buy_price * qty_to_close
                new_realized = realized_pnl + (proceeds - cost_basis)
                new_total_cost = max(total_cost - cost_basis, 0.0)
                new_avg = avg_buy_price if remaining_qty > 0 else None
                values = {
                    "qty": remaining_qty,
                    "avg_buy_price": new_avg,
                    "total_cost": new_total_cost,
                    "realized_pnl": new_realized,
                    "market": market,
                    "last_trade_at": filled_at,
                }

            if row:
                conn.execute(
                    sa.update(self._schema.positions)
                    .where(self._schema.positions.c.position_id == row["position_id"])
                    .values(**values, updated_at=func.now()),
                )
            else:
                conn.execute(
                    sa.insert(self._schema.positions).values(
                        position_id=_coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                        env=env,
                        strategy=strategy,
                        sid=sid,
                        mode=mode,
                        code=code,
                        market=market,
                        qty=values["qty"],
                        avg_buy_price=values["avg_buy_price"],
                        total_cost=values["total_cost"],
                        realized_pnl=values["realized_pnl"],
                        last_trade_at=filled_at,
                        entry_reason=values.get("entry_reason"),
                        entry_style_selected=values.get("entry_style_selected"),
                        entry_decision_family=values.get("entry_decision_family"),
                        entry_rule_version=values.get("entry_rule_version"),
                        entry_meta_json=values.get("entry_meta_json", {}),
                        stop_price_at_entry=values.get("stop_price_at_entry"),
                        pivot_price_at_entry=values.get("pivot_price_at_entry"),
                        exit_policy_family=values.get("exit_policy_family"),
                    )
                )

    def bootstrap_from_kis_holdings(
        self, env: str, strategy: str, sid: int, mode: int, holdings: Iterable[dict]
    ) -> int:
        count = 0
        with self.engine.begin() as conn:
            for row in holdings or []:
                try:
                    code = str(row.get("pdno") or row.get("code") or "").zfill(6)
                    qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
                    if qty <= 0 or not code:
                        continue
                    avg_price = float(row.get("pchs_avg_pric") or row.get("pchs_avg_price") or row.get("avg_price") or 0.0)
                    total_cost = float(row.get("pchs_amt") or row.get("total_cost") or avg_price * qty)
                    market = row.get("prdt_type_cd") or row.get("market") or row.get("mket_gb")
                except Exception:
                    continue
                existing = conn.execute(
                    select(self._schema.positions.c.position_id).where(
                        and_(
                            self._schema.positions.c.env == env,
                            self._schema.positions.c.strategy == strategy,
                            self._schema.positions.c.sid == sid,
                            self._schema.positions.c.mode == mode,
                            self._schema.positions.c.code == code,
                        )
                    )
                ).scalar()
                if existing:
                    continue
                conn.execute(
                    sa.insert(self._schema.positions).values(
                        position_id=_coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                        env=env,
                        strategy=strategy,
                        sid=sid,
                        mode=mode,
                        code=code,
                        market=market,
                        qty=qty,
                        avg_buy_price=avg_price or None,
                        total_cost=total_cost or 0.0,
                        realized_pnl=0.0,
                        last_trade_at=None,
                        status="OPEN",
                        last_reconciled_at=func.now(),
                    )
                )
                count += 1
        return count

    def restore_missing_from_holdings(
        self,
        *,
        env: str,
        strategy: str,
        sid: int,
        mode: int,
        holdings: Iterable[dict],
    ) -> int:
        restored = 0
        with self.engine.begin() as conn:
            for row in holdings or []:
                try:
                    code = str(row.get("pdno") or row.get("code") or "").zfill(6)
                    qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
                    if qty <= 0 or not code:
                        continue
                    avg_price = float(row.get("pchs_avg_pric") or row.get("pchs_avg_price") or row.get("avg_price") or 0.0)
                    total_cost = float(row.get("pchs_amt") or row.get("total_cost") or avg_price * qty)
                    market = row.get("prdt_type_cd") or row.get("market") or row.get("mket_gb")
                except Exception:
                    continue
                existing = conn.execute(
                    select(self._schema.positions.c.position_id).where(
                        and_(
                            self._schema.positions.c.env == env,
                            self._schema.positions.c.strategy == strategy,
                            self._schema.positions.c.sid == sid,
                            self._schema.positions.c.mode == mode,
                            self._schema.positions.c.code == code,
                        )
                    )
                ).scalar()
                if existing:
                    continue
                conn.execute(
                    sa.insert(self._schema.positions).values(
                        position_id=_coerce_uuid(None, uses_native_uuid=self._schema.uses_native_uuid, database_url=str(self.engine.url)),
                        env=env,
                        strategy=strategy,
                        sid=sid,
                        mode=mode,
                        code=code,
                        market=market,
                        qty=qty,
                        avg_buy_price=avg_price or None,
                        total_cost=total_cost or 0.0,
                        realized_pnl=0.0,
                        last_trade_at=None,
                        status="OPEN",
                        last_reconciled_at=func.now(),
                    )
                )
                restored += 1
        return restored


class PracticeAccountResetRepo:
    DELETE_ORDER = (
        "fills",
        "orders",
        "positions",
        "cooldowns",
        "account_snapshot",
        "holdings_snapshot",
        "portfolio_state",
        "open_orders",
        "trade_state",
        "entry_state",
        "exit_state",
    )
    _CORE_TABLES = {
        "positions": "positions",
        "orders": "orders",
        "fills": "fills",
    }
    _OPTIONAL_TABLES = (
        "cooldowns",
        "account_snapshot",
        "holdings_snapshot",
        "portfolio_state",
        "open_orders",
        "trade_state",
        "entry_state",
        "exit_state",
    )

    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)
        self._optional_table_cache: dict[str, sa.Table | None] = {}

    def _normalize_env(self, env: str) -> str:
        env_name = str(env or "").strip().lower()
        if env_name != "practice":
            raise RuntimeError(f"Practice account reset is only allowed for env=practice, got env={env_name or 'unknown'}")
        return env_name

    def _reflect_optional_table(self, table_name: str) -> sa.Table | None:
        cached = self._optional_table_cache.get(table_name)
        if table_name in self._optional_table_cache:
            return cached
        try:
            inspector = sa.inspect(self.engine)
            if not inspector.has_table(table_name):
                logger.warning("[ACCOUNT_RESET][SKIP] table=%s reason=missing", table_name)
                self._optional_table_cache[table_name] = None
                return None
            table = sa.Table(table_name, sa.MetaData(), autoload_with=self.engine)
        except Exception as exc:
            logger.warning("[ACCOUNT_RESET][SKIP] table=%s reason=reflect_failed err=%s", table_name, exc)
            self._optional_table_cache[table_name] = None
            return None
        self._optional_table_cache[table_name] = table
        return table

    def _resolve_table(self, logical_name: str) -> tuple[sa.Table | None, bool]:
        attr_name = self._CORE_TABLES.get(logical_name)
        if attr_name is not None:
            return getattr(self._schema, attr_name), True
        return self._reflect_optional_table(logical_name), False

    def _account_state_conditions(self, table: sa.Table, *, env: str, account_key: str | None) -> list[Any]:
        conditions: list[Any] = []
        if "env" in table.c:
            conditions.append(func.lower(sa.cast(table.c.env, sa.String)) == env)
        for column_name in ("account_env", "strategy_env"):
            if column_name in table.c:
                conditions.append(func.lower(sa.cast(table.c[column_name], sa.String)) == env)
        if account_key and "account_key" in table.c:
            conditions.append(sa.cast(table.c.account_key, sa.String) == str(account_key))
        return conditions

    def _table_names(self) -> tuple[str, ...]:
        return self.DELETE_ORDER

    def _resolved_tables(self) -> list[tuple[str, sa.Table | None, bool]]:
        return [(table_name, *self._resolve_table(table_name)) for table_name in self._table_names()]

    def _count_table(
        self,
        conn: sa.Connection,
        table: sa.Table | None,
        *,
        table_name: str,
        env: str,
        account_key: str | None,
        required: bool,
    ) -> int:
        if table is None:
            if required:
                raise RuntimeError(f"Required practice reset table missing: {table_name}")
            return 0
        conditions = self._account_state_conditions(table, env=env, account_key=account_key)
        if not conditions:
            if required:
                raise RuntimeError(f"Required practice reset table has no safe scope filter: {table_name}")
            logger.warning("[ACCOUNT_RESET][SKIP] table=%s reason=unsafe_scope", table_name)
            return 0
        stmt = select(func.count()).select_from(table).where(and_(*conditions))
        return int(conn.execute(stmt).scalar() or 0)

    def count_account_state_rows(self, env: str, account_key: str | None = None) -> dict[str, int]:
        env_name = self._normalize_env(env)
        resolved_tables = self._resolved_tables()
        counts: dict[str, int] = {}
        with self.engine.begin() as conn:
            for table_name, table, required in resolved_tables:
                counts[table_name] = self._count_table(
                    conn,
                    table,
                    table_name=table_name,
                    env=env_name,
                    account_key=account_key,
                    required=required,
                )
        return counts

    def clear_account_state(self, env: str, account_key: str | None = None) -> dict[str, int]:
        env_name = self._normalize_env(env)
        resolved_tables = self._resolved_tables()
        cleared_counts: dict[str, int] = {}
        current_table_name = ""
        with self.engine.begin() as conn:
            try:
                for table_name, table, required in resolved_tables:
                    current_table_name = table_name
                    if table is None:
                        if required:
                            raise RuntimeError(f"Required practice reset table missing: {table_name}")
                        cleared_counts[table_name] = 0
                        continue
                    conditions = self._account_state_conditions(table, env=env_name, account_key=account_key)
                    if not conditions:
                        if required:
                            raise RuntimeError(f"Required practice reset table has no safe scope filter: {table_name}")
                        logger.warning("[ACCOUNT_RESET][SKIP] table=%s reason=unsafe_scope", table_name)
                        cleared_counts[table_name] = 0
                        continue
                    result = conn.execute(sa.delete(table).where(and_(*conditions)))
                    cleared_counts[table_name] = int(result.rowcount or 0)
                    logger.info(
                        "[ACCOUNT_RESET][CLEAR] table=%s rows=%s",
                        table_name,
                        cleared_counts[table_name],
                    )
            except IntegrityError as exc:
                logger.exception(
                    "[ACCOUNT_RESET][FAIL][FK] table=%s reason=integrity_error err=%s",
                    current_table_name or "unknown",
                    exc,
                )
                raise
        return cleared_counts

    def insert_reset_ledger_event(
        self,
        env: str,
        account_key: str,
        capital_krw: int,
        cleared_counts: dict[str, int],
    ) -> str | None:
        env_name = self._normalize_env(env)
        ledger_repo = LedgerEventsRepo(self.engine)
        return ledger_repo.append_event(
            env=env_name,
            run_id=None,
            strategy="pb1_pullback_close",
            run_window="reset",
            event_type="PRACTICE_ACCOUNT_RESET",
            ts=now_kst(),
            ok=True,
            stage="ACCOUNT_RESET",
            payload_json={
                "status": "DONE",
                "account_key": account_key,
                "capital_krw": int(capital_krw),
                "cleared_counts": {key: int(value or 0) for key, value in (cleared_counts or {}).items()},
            },
        )


class ReconcileLogRepo:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def append_log(
        self,
        *,
        env: str,
        strategy: str,
        tick_ts: datetime,
        action: str,
        details_json: dict | None,
    ) -> None:
        tbl = self._schema.reconcile_log
        # 1) payload를 항상 생성
        payload = {
            "env": env,
            "strategy": strategy,
            "tick_ts": tick_ts,
            "action": action,
            "details_json": json_sanitize(details_json or {}),
        }
        # 2) 실제 테이블 컬럼만 남김
        cols = set(tbl.c.keys())
        payload = {k: v for k, v in payload.items() if k in cols}
        try:
            stmt = sa.insert(tbl).values(**payload)
            with self.engine.begin() as conn:
                conn.execute(stmt)
        except Exception:
            logger.exception("[RECONCILE_LOG][APPEND][FAIL] payload_keys=%s", list(payload.keys()))

    def append_log_from_context(
        self,
        ctx: RunContext,
        action: str,
        tick_ts: datetime,
        details_json: dict | None = None,
    ) -> None:
        assert_uuid(ctx.run_id)
        tbl = self._schema.reconcile_log
        # 1) payload를 항상 생성
        payload = {
            "env": ctx.env,
            "strategy": ctx.strategy,
            "tick_ts": tick_ts,
            "action": action,
            "details_json": json_sanitize(details_json or {}),
        }
        # 2) 실제 테이블 컬럼만 남김
        cols = set(tbl.c.keys())
        payload = {k: v for k, v in payload.items() if k in cols}
        try:
            stmt = sa.insert(tbl).values(**payload)
            with self.engine.begin() as conn:
                conn.execute(stmt)
        except Exception:
            logger.exception("[RECONCILE_LOG][APPEND][FAIL] payload_keys=%s", list(payload.keys()))


def load_price_daily_conn(conn: sa.Connection, code: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
    schema = schema_for_engine(conn.engine)
    stmt = sa.select(schema.price_daily).where(
        and_(
            schema.price_daily.c.code == code,
            schema.price_daily.c.date >= start_date,
            schema.price_daily.c.date <= end_date,
        )
    ).order_by(schema.price_daily.c.date)
    result = conn.execute(stmt)
    rows = result.fetchall()
    return [
        {
            "date": row.date.strftime("%Y%m%d"),
            "open": float(row.open) if row.open else None,
            "high": float(row.high) if row.high else None,
            "low": float(row.low) if row.low else None,
            "close": float(row.close) if row.close else None,
            "volume": float(row.volume) if row.volume else None,
            "value": float(row.value) if row.value else None,
        }
        for row in rows
    ]


def load_price_daily(engine: Engine, code: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
    def _op() -> List[Dict[str, Any]]:
        with engine.connect() as conn:
            return load_price_daily_conn(conn, code, start_date, end_date)

    return run_with_db_retry(
        engine,
        fn=_op,
        operation="load_price_daily",
        max_attempts=5,
    )


def upsert_price_daily_conn(conn: sa.Connection, candles: List[Dict[str, Any]], market: str, code: str) -> None:
    if not candles:
        return
    schema = schema_for_engine(conn.engine)
    for candle in candles:
        payload = {
            "market": market,
            "code": code,
            "date": candle["date"],
            "open": candle.get("open"),
            "high": candle.get("high"),
            "low": candle.get("low"),
            "close": candle.get("close"),
            "volume": candle.get("volume"),
            "value": candle.get("value"),
            "source": "KIS",
        }
        conflict_cols = ["market", "code", "date"]
        update_cols = {k: v for k, v in payload.items() if k not in conflict_cols}

        if conn.dialect.name == "postgresql":
            stmt = pg_insert(schema.price_daily).values(**payload).on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_cols
            )
            conn.execute(stmt)
        else:
            try:
                conn.execute(sa.insert(schema.price_daily).values(**payload))
            except IntegrityError:
                where_clause = and_(
                    schema.price_daily.c.market == market,
                    schema.price_daily.c.code == code,
                    schema.price_daily.c.date == candle["date"]
                )
                conn.execute(sa.update(schema.price_daily).where(where_clause).values(**update_cols))


def upsert_price_daily(engine: Engine, candles: List[Dict[str, Any]], market: str, code: str) -> None:
    if not candles:
        return

    def _op() -> None:
        with engine.begin() as conn:
            upsert_price_daily_conn(conn, candles, market, code)

    run_with_db_retry(
        engine,
        fn=_op,
        operation="upsert_price_daily",
        max_attempts=5,
    )


# Backward compatibility alias
PositionRepo = PositionsRepo


# ========================================
# PB1 Watchlist Repository
# ========================================

class WatchlistRepo:
    """PB1 Watchlist 전용 repo."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)
    
    def save_watchlist(
        self,
        *,
        env: str,
        strategy: str,
        as_of: date,
        members: List[Dict[str, Any]],
    ) -> None:
        """
        Watchlist를 DB에 upsert.
        members: [{"code": "005930", "rank": 1, "score": 75.5, "meta": {...}}, ...]
        """
        if _norm_strategy(strategy) in SCORED_WATCHLIST_STRATEGIES:
            save_pb1_watchlist_rows(
                self.engine,
                env=env,
                strategy=strategy,
                as_of=as_of,
                rows=members,
            )
            return

        save_pb1_watchlist_rows(
            self.engine,
            env=env,
            strategy=strategy,
            as_of=as_of,
            rows=members,
        )
    
    def load_watchlist(
        self,
        *,
        env: str,
        strategy: str,
        as_of: date | str,
        allow_latest_fallback: bool = True,
        ttl_days: int = 7,
        max_back_days: int = 3,
    ) -> tuple[List[Dict[str, Any]], date | None]:
        """
        특정 날짜의 watchlist 조회.
        반환: ([{"code": "005930", "rank": 1, "score": 75.5, "meta": {...}}, ...], used_as_of)
        
        ✅ as_of는 DATE 타입으로 강제 변환 (VARCHAR 캐스팅 방지)
        ✅ allow_latest_fallback=True: as_of에 없으면 TTL 이내 최신 as_of 사용
        """
        if _norm_strategy(strategy) == "pb1_watchlist_final_scored":
            return self.load_watchlist_scored(
                env=env,
                strategy=strategy,
                as_of=as_of,
                allow_latest_fallback=allow_latest_fallback,
                ttl_days=ttl_days,
                max_back_days=max_back_days,
            )

        as_of_date = to_date(as_of)
        env_n = _norm_env(env)
        strategy_n = _norm_strategy(strategy)
        effective_max_back_days = int(max_back_days) if allow_latest_fallback else 0

        codes = load_pb1_watchlist_codes(
            self.engine,
            env=env_n,
            strategy=strategy_n,
            as_of=as_of_date,
            ttl_days=ttl_days,
            max_back_days=effective_max_back_days,
        )
        if not codes:
            return [], None

        schema = self._schema
        with self.engine.connect() as conn:
            exact_count_stmt = (
                select(func.count())
                .select_from(schema.pb1_watchlist)
                .where(
                    and_(
                        schema.pb1_watchlist.c.env == env_n,
                        schema.pb1_watchlist.c.strategy == strategy_n,
                        schema.pb1_watchlist.c.as_of == as_of_date,
                    )
                )
            )
            exact_count = int(conn.execute(exact_count_stmt).scalar() or 0)

            used_as_of = as_of_date if exact_count > 0 else None
            if used_as_of is None and allow_latest_fallback:
                effective_back_days = max(0, min(int(ttl_days), int(max_back_days)))
                min_date = as_of_date - timedelta(days=effective_back_days)
                latest_stmt = (
                    select(func.max(schema.pb1_watchlist.c.as_of))
                    .where(
                        and_(
                            schema.pb1_watchlist.c.env == env_n,
                            schema.pb1_watchlist.c.strategy == strategy_n,
                            schema.pb1_watchlist.c.as_of <= as_of_date,
                            schema.pb1_watchlist.c.as_of >= min_date,
                        )
                    )
                )
                used_as_of = conn.execute(latest_stmt).scalar()

            if used_as_of is None:
                return [], None

            stmt = (
                select(schema.pb1_watchlist)
                .where(
                    and_(
                        schema.pb1_watchlist.c.env == env_n,
                        schema.pb1_watchlist.c.strategy == strategy_n,
                        schema.pb1_watchlist.c.as_of == used_as_of,
                    )
                )
                .order_by(schema.pb1_watchlist.c.rank)
            )
            rows = conn.execute(stmt).fetchall()

        result = [
            {
                "code": row.code,
                "rank": row.rank,
                "score": float(row.score) if row.score is not None else None,
                "meta": row.meta,
            }
            for row in rows
        ]
        logger.info(
            "[WATCHLIST][LOAD] strategy=%s members=%s",
            strategy_n,
            len(result),
        )
        return result, used_as_of

    def count_watchlist(
        self,
        *,
        env: str,
        strategy: str,
        as_of: date | str,
    ) -> int:
        env_n = _norm_env(env)
        strategy_n = _norm_strategy(strategy)
        as_of_date = to_date(as_of)
        schema = self._schema
        with self.engine.connect() as conn:
            stmt = (
                select(func.count())
                .select_from(schema.pb1_watchlist)
                .where(
                    and_(
                        schema.pb1_watchlist.c.env == env_n,
                        schema.pb1_watchlist.c.strategy == strategy_n,
                        schema.pb1_watchlist.c.as_of == as_of_date,
                    )
                )
            )
            return int(conn.execute(stmt).scalar() or 0)

    def count_as_of(
        self,
        *,
        env: str,
        as_of: date | str,
        strategy: str = "pb1_watchlist_final_scored",
    ) -> int:
        return self.count_watchlist(
            env=env,
            strategy=strategy,
            as_of=as_of,
        )

    def verify_watchlist_scored_contract(
        self,
        *,
        env: str,
        as_of: date | str,
        strategy: str = "pb1_watchlist_final_scored",
        allow_latest_fallback: bool = False,
        ttl_days: int = 7,
        max_back_days: int = 3,
        expected_rows: int = FINAL30_SCORED_REQUIRED_ROWS,
        log_result: bool = True,
    ) -> Dict[str, Any]:
        rows, used_as_of = self.load_watchlist_scored(
            env=env,
            strategy=strategy,
            as_of=as_of,
            allow_latest_fallback=allow_latest_fallback,
            ttl_days=ttl_days,
            max_back_days=max_back_days,
        )
        summary = summarize_final30_scored_contract(
            rows,
            env=env,
            as_of=as_of,
            expected_rows=expected_rows,
        )
        summary["used_as_of"] = used_as_of.isoformat() if used_as_of is not None else None
        summary["strategy"] = _norm_strategy(strategy)
        summary["field_null_counts"] = _field_null_counts(summary.get("rows_data") or [], FINAL30_SCORED_DB_CONTRACT_FIELDS)
        if log_result:
            logger.info(
                "[DB][FINAL30_SCORED][VERIFY] rows=%s uniq_codes=%s uniq_ranks=%s rank_source=%s rank_warn=%s null_critical=%s null_warnings=%s zero_invalid=%s atr_zero_invalid=%s env=%s as_of=%s ok=%s",
                summary["rows"],
                summary["uniq_codes"],
                summary["uniq_ranks"],
                summary["rank_source"],
                int(summary["rank_warn"]),
                summary["null_critical"],
                summary.get("null_warnings", 0),
                summary.get("critical_numeric_zero_invalid_count", 0),
                summary.get("atr_pct_zero_invalid_when_close_positive", 0),
                summary["env"],
                summary["as_of"],
                int(summary["ok"]),
            )
            logger.info("[DB][FINAL30_SCORED][VERIFY][FIELD_NULL_COUNTS] %s", summary.get("field_null_counts", {}))
            if summary.get("null_warnings"):
                logger.warning(
                    "[DB][FINAL30_SCORED][VERIFY][NULL_WARN] env=%s as_of=%s null_warnings=%s nullable_fields=%s",
                    summary["env"],
                    summary["as_of"],
                    summary.get("null_warnings", 0),
                    sorted(FINAL30_SCORED_NULLABLE_FIELDS),
                )
        return summary

    def load_watchlist_scored(
        self,
        *,
        env: str,
        strategy: str = "pb1_watchlist_final_scored",
        as_of: date | str,
        allow_latest_fallback: bool = True,
        ttl_days: int = 7,
        max_back_days: int = 3,
        require_exact_rows: int | None = None,
        require_scored: bool = False,
        fail_if_missing: bool = False,
    ) -> tuple[List[Dict[str, Any]], date | None]:
        as_of_date = to_date(as_of)
        env_n = _norm_env(env)
        strategy_n = _norm_strategy(strategy)
        effective_max_back_days = int(max_back_days) if allow_latest_fallback else 0
        strict_mode = bool(require_scored or fail_if_missing or require_exact_rows is not None)
        expected_strategy = "pb1_watchlist_final_scored"
        logger.info(
            "[DB][FINAL30_SCORED][LOAD_START] env=%s as_of=%s strategy=%s strict=%s allow_latest_fallback=%s require_exact_rows=%s require_scored=%s fail_if_missing=%s",
            env_n,
            as_of_date.isoformat(),
            strategy_n,
            int(strict_mode),
            int(bool(allow_latest_fallback)),
            require_exact_rows,
            int(bool(require_scored)),
            int(bool(fail_if_missing)),
        )

        if strict_mode and require_scored and strategy_n != expected_strategy:
            logger.error(
                "[WATCHLIST][LOAD_SCORED][STRICT_FAIL] expected_strategy=%s actual_strategy=%s rows=0 has_required_cols=0 missing_cols=%s exact_rows_ok=0 required_scored_ok=0 reason=strategy_mismatch",
                expected_strategy,
                strategy_n,
                list(REQUIRED_FINAL30_SCORED_COLS),
            )
            raise ScoredWatchlistInvalidError(
                "strategy_mismatch",
                expected_strategy=expected_strategy,
                actual_strategy=strategy_n,
                rows=0,
                missing_cols=REQUIRED_FINAL30_SCORED_COLS,
            )

        codes = load_pb1_watchlist_codes(
            self.engine,
            env=env_n,
            strategy=strategy_n,
            as_of=as_of_date,
            ttl_days=ttl_days,
            max_back_days=effective_max_back_days,
        )
        if not codes:
            logger.info("[WATCHLIST][LOAD_SCORED] strategy=%s rows=0 cols=[]", strategy_n)
            logger.info(
                "[WATCHLIST][LOAD_SCORED][CHECK] has_score_final=0 has_tech_score=0 has_breakout_score=0 has_pullback_score=0 has_momentum_score=0 has_rs_percentile=0 has_vcp_score=0 has_entry_style_selected=0"
            )
            if strict_mode and fail_if_missing:
                logger.error(
                    "[WATCHLIST][LOAD_SCORED][STRICT_FAIL] expected_strategy=%s actual_strategy=none rows=0 has_required_cols=0 missing_cols=%s exact_rows_ok=0 required_scored_ok=0 reason=scored_final30_missing",
                    expected_strategy,
                    list(REQUIRED_FINAL30_SCORED_COLS),
                )
                raise ScoredWatchlistNotFoundError(
                    "scored_final30_missing",
                    expected_strategy=expected_strategy,
                    actual_strategy="none",
                    rows=0,
                    missing_cols=REQUIRED_FINAL30_SCORED_COLS,
                )
            return [], None

        schema = self._schema
        with self.engine.connect() as conn:
            exact_count_stmt = (
                select(func.count())
                .select_from(schema.pb1_watchlist)
                .where(
                    and_(
                        schema.pb1_watchlist.c.env == env_n,
                        schema.pb1_watchlist.c.strategy == strategy_n,
                        schema.pb1_watchlist.c.as_of == as_of_date,
                    )
                )
            )
            exact_count = int(conn.execute(exact_count_stmt).scalar() or 0)
            logger.info(
                "[DB][FINAL30_SCORED][EXACT_COUNT] env=%s as_of=%s strategy=%s rows=%s",
                env_n,
                as_of_date.isoformat(),
                strategy_n,
                exact_count,
            )

            used_as_of = as_of_date if exact_count > 0 else None
            if used_as_of is None and allow_latest_fallback:
                effective_back_days = max(0, min(int(ttl_days), int(max_back_days)))
                min_date = as_of_date - timedelta(days=effective_back_days)
                latest_stmt = (
                    select(func.max(schema.pb1_watchlist.c.as_of))
                    .where(
                        and_(
                            schema.pb1_watchlist.c.env == env_n,
                            schema.pb1_watchlist.c.strategy == strategy_n,
                            schema.pb1_watchlist.c.as_of <= as_of_date,
                            schema.pb1_watchlist.c.as_of >= min_date,
                        )
                    )
                )
                used_as_of = conn.execute(latest_stmt).scalar()

            if used_as_of is None:
                logger.info(
                    "[DB][FINAL30_SCORED][LOAD_RESULT] source_name=none rows=0 is_scored=0 contract_ok=0 missing_critical_fields=%s",
                    list(REQUIRED_FINAL30_SCORED_COLS),
                )
                return [], None

            stmt = (
                select(schema.pb1_watchlist)
                .where(
                    and_(
                        schema.pb1_watchlist.c.env == env_n,
                        schema.pb1_watchlist.c.strategy == strategy_n,
                        schema.pb1_watchlist.c.as_of == used_as_of,
                    )
                )
                .order_by(schema.pb1_watchlist.c.rank)
            )
            raw_rows = conn.execute(stmt).fetchall()

        if strict_mode and strategy_n == expected_strategy and used_as_of == as_of_date and exact_count > 0 and not raw_rows:
            logger.error(
                "[DB][FINAL30_SCORED][STRICT_FAIL] reason=exact_contract_empty env=%s as_of=%s strategy=%s exact_count=%s",
                env_n,
                as_of_date.isoformat(),
                strategy_n,
                exact_count,
            )
            raise ScoredWatchlistInvalidError(
                "exact_contract_empty",
                expected_strategy=expected_strategy,
                actual_strategy=strategy_n,
                rows=0,
                missing_cols=REQUIRED_FINAL30_SCORED_COLS,
            )

        result = [_normalize_scored_loaded_row(row) for row in raw_rows]
        actual_strategy_values = sorted(
            {
                _norm_strategy(getattr(row, "strategy", None))
                for row in raw_rows
                if str(getattr(row, "strategy", "") or "").strip()
            }
        )
        if not actual_strategy_values:
            actual_strategy = strategy_n if result else "none"
        elif len(actual_strategy_values) == 1:
            actual_strategy = actual_strategy_values[0]
        else:
            actual_strategy = ",".join(actual_strategy_values)
        cols = sorted({k for item in result for k in item.keys()})
        has = {col: int(col in cols) for col in CRITICAL_SCORED_COLS}
        numeric_restore = {
            field: sum(1 for row in result if safe_nullable_float((row or {}).get(field)) is not None)
            for field in FINAL30_NUMERIC_RESTORE_ALIASES
        }
        ma20_null_count = sum(1 for row in result if safe_nullable_float((row or {}).get("ma20")) is None)
        ma20_positive_count = sum(
            1
            for row in result
            if safe_nullable_float((row or {}).get("ma20")) is not None and float((row or {}).get("ma20") or 0.0) > 0
        )
        logger.info(
            "[WATCHLIST][LOAD_SCORED] strategy=%s rows=%s cols=%s",
            strategy_n,
            len(result),
            cols,
        )
        logger.info("[WATCHLIST][LOAD_SCORED][NUMERIC_RESTORE] counts=%s", numeric_restore)
        logger.info(
            "[WATCHLIST][LOAD_SCORED][MA20_STATS] ma20_null_count=%s ma20_positive_count=%s",
            ma20_null_count,
            ma20_positive_count,
        )
        logger.info(
            "[WATCHLIST][LOAD_SCORED][CHECK] has_score_final=%s has_tech_score=%s has_breakout_score=%s has_pullback_score=%s has_momentum_score=%s has_rs_percentile=%s has_vcp_score=%s has_entry_style_selected=%s",
            has.get("score_final", 0),
            has.get("tech_score", 0),
            has.get("breakout_score", 0),
            has.get("pullback_score", 0),
            has.get("momentum_score", 0),
            has.get("rs_percentile", 0),
            has.get("vcp_score", 0),
            has.get("entry_style_selected", 0),
        )
        logger.info("[DB][FINAL30_SCORED][LOAD_VERIFY] rows=%s cols=%s", len(result), cols)
        missing_loaded_fields = [field for field in REQUIRED_FINAL30_SCORED_COLS if field not in cols]
        logger.info("[DB][FINAL30_SCORED][LOAD_VERIFY_MISSING] missing=%s", missing_loaded_fields)
        logger.info(
            "[DB][FINAL30_SCORED][LOAD_RESULT] source_name=%s rows=%s is_scored=%s contract_ok=%s missing_critical_fields=%s",
            "db_pb1_watchlist_final_scored" if result else "none",
            len(result),
            int(len(missing_loaded_fields) == 0),
            int((len(result) == FINAL30_SCORED_REQUIRED_ROWS) and (len(missing_loaded_fields) == 0) and actual_strategy == expected_strategy),
            missing_loaded_fields,
        )
        exact_rows_ok = True if require_exact_rows is None else len(result) == int(require_exact_rows)
        required_scored_ok = len(missing_loaded_fields) == 0
        strategy_ok = actual_strategy == expected_strategy
        if strict_mode:
            if strategy_ok and exact_rows_ok and required_scored_ok:
                logger.info(
                    "[WATCHLIST][LOAD_SCORED][STRICT] expected_strategy=%s actual_strategy=%s rows=%s exact_rows_ok=1 required_scored_ok=1 has_required_cols=1 missing_cols=[]",
                    expected_strategy,
                    actual_strategy,
                    len(result),
                )
            else:
                if not strategy_ok:
                    reason = "strategy_mismatch"
                elif len(result) == 0:
                    reason = "scored_final30_missing"
                elif not exact_rows_ok:
                    reason = "rows_not_30"
                else:
                    reason = "required_scored_cols_missing"
                logger.error(
                    "[WATCHLIST][LOAD_SCORED][STRICT_FAIL] expected_strategy=%s actual_strategy=%s rows=%s has_required_cols=%s missing_cols=%s exact_rows_ok=%s required_scored_ok=%s reason=%s",
                    expected_strategy,
                    actual_strategy,
                    len(result),
                    int(required_scored_ok),
                    missing_loaded_fields,
                    int(exact_rows_ok),
                    int(required_scored_ok),
                    reason,
                )
                error_cls = ScoredWatchlistNotFoundError if reason == "scored_final30_missing" else ScoredWatchlistInvalidError
                raise error_cls(
                    reason,
                    expected_strategy=expected_strategy,
                    actual_strategy=actual_strategy,
                    rows=len(result),
                    missing_cols=missing_loaded_fields,
                )
        if result:
            _log_scored_sample("[WATCHLIST][LOAD_SCORED][POST_DB_SAMPLE]", result)
            sample_keys = sorted(result[0].keys())
            sample_row = {k: result[0].get(k) for k in CRITICAL_SCORED_COLS + ["code", "rank_final30", "score"] if k in result[0]}
            logger.info("[WATCHLIST][LOAD_SCORED][SAMPLE_KEYS] keys=%s", sample_keys)
            logger.info("[WATCHLIST][LOAD_SCORED][SAMPLE_ROW] %s", sample_row)
        if len(result) == FINAL30_SCORED_REQUIRED_ROWS and not missing_loaded_fields:
            logger.info("[DB][FINAL30_SCORED][ROUNDTRIP_OK] rows=%s usable=1", len(result))
        return result, used_as_of

    def load_final30_scored_exact(
        self,
        *,
        env: str,
        as_of: date | str,
        strategy: str = "pb1_watchlist_final_scored",
        require_rows: int = FINAL30_SCORED_REQUIRED_ROWS,
        require_scored: bool = True,
        fail_if_missing: bool = False,
    ) -> pd.DataFrame:
        env_n = _norm_env(env)
        as_of_date = to_date(as_of)
        strategy_n = _norm_strategy(strategy) or "pb1_watchlist_final_scored"
        rows, used_as_of = self.load_watchlist_scored(
            env=env_n,
            strategy=strategy_n,
            as_of=as_of_date,
            allow_latest_fallback=False,
            require_exact_rows=require_rows,
            require_scored=require_scored,
            fail_if_missing=fail_if_missing,
        )

        exact_count = len(rows) if used_as_of == as_of_date else 0
        logger.info(
            "[DB][FINAL30_SCORED][EXACT_COUNT] env=%s as_of=%s rows=%s",
            env_n,
            as_of_date.isoformat(),
            exact_count,
        )

        df = pd.DataFrame(list(rows or []))
        if not df.empty:
            if "rank_final30" not in df.columns and "rank" in df.columns:
                df["rank_final30"] = df["rank"]
            if "as_of" not in df.columns:
                df["as_of"] = as_of_date.isoformat()
            else:
                df["as_of"] = df["as_of"].astype(str)
            if "code" in df.columns:
                df["code"] = df["code"].astype(str).str.zfill(6)

        columns = [str(col) for col in df.columns.tolist()]
        missing_critical_fields = [
            field
            for field in CRITICAL_SCORED_COLS
            if field not in set(columns)
        ]
        contract_ok = int(len(df) == int(require_rows) and not missing_critical_fields)
        usable = int(contract_ok == 1)
        logger.info(
            "[DB][FINAL30_SCORED][LOAD_VERIFY] rows=%s cols=%s",
            len(df),
            columns,
        )
        logger.info(
            "[DB][FINAL30_SCORED][LOAD_RESULT] source_name=%s rows=%s is_scored=%s contract_ok=%s usable=%s missing_critical_fields=%s",
            "db_pb1_watchlist_final_scored" if not df.empty else "none",
            len(df),
            int(not missing_critical_fields),
            contract_ok,
            usable,
            missing_critical_fields,
        )

        if fail_if_missing and df.empty:
            raise ScoredWatchlistNotFoundError(
                "scored_final30_missing",
                expected_strategy="pb1_watchlist_final_scored",
                actual_strategy=strategy_n,
                rows=0,
                missing_cols=CRITICAL_SCORED_COLS,
            )
        if fail_if_missing and len(df) != int(require_rows):
            raise ScoredWatchlistInvalidError(
                "rows_not_30",
                expected_strategy="pb1_watchlist_final_scored",
                actual_strategy=strategy_n,
                rows=len(df),
                missing_cols=missing_critical_fields,
            )
        if fail_if_missing and missing_critical_fields:
            raise ScoredWatchlistInvalidError(
                "required_scored_cols_missing",
                expected_strategy="pb1_watchlist_final_scored",
                actual_strategy=strategy_n,
                rows=len(df),
                missing_cols=missing_critical_fields,
            )
        return df
    
    def get_latest_watchlist_date(
        self,
        *,
        env: str,
        strategy: str,
    ) -> Optional[date]:
        """가장 최근 watchlist의 as_of 날짜 반환."""
        schema = self._schema
        env_n = _norm_env(env)
        strategy_n = _norm_strategy(strategy)
        with self.engine.connect() as conn:
            stmt = (
                select(func.max(schema.pb1_watchlist.c.as_of))
                .where(
                    and_(
                        schema.pb1_watchlist.c.env == env_n,
                        schema.pb1_watchlist.c.strategy == strategy_n,
                    )
                )
            )
            result = conn.execute(stmt).scalar()
        return result


def load_watchlist_scored(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date | str,
    allow_latest_fallback: bool = True,
    ttl_days: int = 7,
    max_back_days: int = 3,
    require_exact_rows: int | None = None,
    require_scored: bool = False,
    fail_if_missing: bool = False,
) -> tuple[List[Dict[str, Any]], date | None]:
    repo = WatchlistRepo(engine)
    return repo.load_watchlist_scored(
        env=env,
        strategy=strategy,
        as_of=as_of,
        allow_latest_fallback=allow_latest_fallback,
        ttl_days=ttl_days,
        max_back_days=max_back_days,
        require_exact_rows=require_exact_rows,
        require_scored=require_scored,
        fail_if_missing=fail_if_missing,
    )


def load_final30_scored_exact(
    engine: Engine,
    *,
    env: str,
    as_of: date | str,
    strategy: str = "pb1_watchlist_final_scored",
    require_rows: int = FINAL30_SCORED_REQUIRED_ROWS,
    require_scored: bool = True,
    fail_if_missing: bool = False,
) -> pd.DataFrame:
    repo = WatchlistRepo(engine)
    return repo.load_final30_scored_exact(
        env=env,
        as_of=as_of,
        strategy=strategy,
        require_rows=require_rows,
        require_scored=require_scored,
        fail_if_missing=fail_if_missing,
    )


def load_final30_scored_db_only(
    engine: Engine,
    *,
    env: str,
    as_of: date | str,
    strategy: str = "pb1_watchlist_final_scored",
    require_exact_rows: int = 30,
    fail_if_missing: bool = True,
) -> pd.DataFrame:
    env_n = _norm_env(env)
    as_of_date = to_date(as_of)
    strategy_n = _norm_strategy(strategy) or "pb1_watchlist_final_scored"
    logger.info(
        "[DB][FINAL30_SCORED][LOAD_START] env=%s as_of=%s strategy=%s strict=1",
        env_n,
        as_of_date.isoformat(),
        strategy_n,
    )

    repo = WatchlistRepo(engine)
    df = repo.load_final30_scored_exact(
        env=env_n,
        as_of=as_of_date,
        strategy=strategy_n,
        require_rows=require_exact_rows,
        require_scored=True,
        fail_if_missing=fail_if_missing,
    )

    cols = [str(col) for col in df.columns.tolist()]
    logger.info("[DB][FINAL30_SCORED][LOAD_VERIFY] rows=%s cols=%s", len(df), cols)

    required_cols = list(FINAL30_SCORED_DB_CONTRACT_FIELDS)
    missing_cols = [col for col in required_cols if col not in set(cols)]
    logger.info("[DB][FINAL30_SCORED][LOAD_VERIFY_MISSING] missing=%s", missing_cols)

    if df.empty:
        logger.error(
            "[DB][FINAL30_SCORED][LOAD_RESULT] source_name=none rows=0 contract_ok=0 reason=scored_final30_missing"
        )
        if fail_if_missing:
            raise ScoredWatchlistNotFoundError(
                "scored_final30_missing",
                expected_strategy="pb1_watchlist_final_scored",
                actual_strategy="none",
                rows=0,
                missing_cols=required_cols,
            )
        return df

    if len(df) != int(require_exact_rows):
        logger.error(
            "[DB][FINAL30_SCORED][LOAD_RESULT] source_name=db_pb1_watchlist_final_scored rows=%s contract_ok=0 reason=rows_not_30",
            len(df),
        )
        raise ScoredWatchlistInvalidError(
            "rows_not_30",
            expected_strategy="pb1_watchlist_final_scored",
            actual_strategy=strategy_n,
            rows=len(df),
            missing_cols=missing_cols,
        )

    if missing_cols:
        logger.error(
            "[DB][FINAL30_SCORED][LOAD_RESULT] source_name=db_pb1_watchlist_final_scored rows=%s contract_ok=0 reason=required_scored_cols_missing",
            len(df),
        )
        raise ScoredWatchlistInvalidError(
            "required_scored_cols_missing",
            expected_strategy="pb1_watchlist_final_scored",
            actual_strategy=strategy_n,
            rows=len(df),
            missing_cols=missing_cols,
        )

    logger.info(
        "[DB][FINAL30_SCORED][LOAD_RESULT] source_name=db_pb1_watchlist_final_scored rows=%s contract_ok=1",
        len(df),
    )
    return df


def verify_final30_scored_contract(
    engine: Engine,
    *,
    env: str,
    as_of: date | str,
    strategy: str = "pb1_watchlist_final_scored",
    allow_latest_fallback: bool = False,
    ttl_days: int = 7,
    max_back_days: int = 3,
    expected_rows: int = FINAL30_SCORED_REQUIRED_ROWS,
    log_result: bool = True,
) -> Dict[str, Any]:
    repo = WatchlistRepo(engine)
    return repo.verify_watchlist_scored_contract(
        env=env,
        as_of=as_of,
        strategy=strategy,
        allow_latest_fallback=allow_latest_fallback,
        ttl_days=ttl_days,
        max_back_days=max_back_days,
        expected_rows=expected_rows,
        log_result=log_result,
    )


# ========================================
# Derived Minervini Repository
# ========================================

class DerivedMinerviniRepo:
    """Daily Minervini/PB1 derived feature snapshots."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def upsert_rows(self, *, env: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        schema = self._schema
        env_n = _norm_env(env)
        normalized_rows = []
        for row in rows:
            payload = dict(row)
            payload["env"] = env_n
            normalized_rows.append(payload)
        with self.engine.begin() as conn:
            if conn.dialect.name == "postgresql":
                stmt = pg_insert(schema.derived_minervini).values(normalized_rows)
                update_cols = {
                    col.name: getattr(stmt.excluded, col.name)
                    for col in schema.derived_minervini.c
                    if col.name not in {"env", "symbol", "as_of", "created_at"}
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        schema.derived_minervini.c.env,
                        schema.derived_minervini.c.symbol,
                        schema.derived_minervini.c.as_of,
                    ],
                    set_=update_cols,
                )
                conn.execute(stmt)
            else:
                for payload in normalized_rows:
                    try:
                        conn.execute(sa.insert(schema.derived_minervini).values(**payload))
                    except IntegrityError:
                        where_clause = and_(
                            schema.derived_minervini.c.env == payload["env"],
                            schema.derived_minervini.c.symbol == payload["symbol"],
                            schema.derived_minervini.c.as_of == payload["as_of"],
                        )
                        update_cols = {k: v for k, v in payload.items() if k not in {"env", "symbol", "as_of"}}
                        conn.execute(sa.update(schema.derived_minervini).where(where_clause).values(**update_cols))
        return len(normalized_rows)

    @staticmethod
    def _to_float(value: object, default: float | None = None) -> float | None:
        out = safe_nullable_float(value)
        if out is None:
            return default
        return out

    def _normalize_derived_row(self, row: dict) -> dict:
        """Guarantee watchlist-facing score fields exist and are numeric."""
        out = dict(row)
        features = out.get("features_json") if isinstance(out.get("features_json"), dict) else {}
        entry_scores = features.get("entry_scores") if isinstance(features, dict) else {}
        if not isinstance(entry_scores, dict):
            entry_scores = {}
        entry_component = features.get("entry_component") if isinstance(features.get("entry_component"), dict) else {}

        rs_percentile = self._to_float(out.get("rs_percentile"))
        out["rs_percentile"] = rs_percentile
        out["rs_score"] = self._to_float(out.get("rs_score"), rs_percentile)

        # Backward-compat: keep vcp_score/trend_score if only in features_json payload.
        out["vcp_score"] = self._to_float(out.get("vcp_score"), self._to_float(features.get("vcp_score")))
        out["trend_score"] = self._to_float(out.get("trend_score"), self._to_float(entry_scores.get("trend_score")))
        out["breakout_score"] = self._to_float(out.get("breakout_score"), self._to_float(entry_scores.get("breakout_score")))
        out["pullback_score"] = self._to_float(out.get("pullback_score"), self._to_float(entry_scores.get("pullback_score")))
        out["momentum_score"] = self._to_float(out.get("momentum_score"), self._to_float(entry_scores.get("momentum_score")))
        out["entry_style_selected"] = out.get("entry_style_selected") or features.get("entry_style_selected") or entry_component.get("selected")
        return out

    def load_for_as_of(self, *, env: str, as_of: date, symbols: list[str] | None = None) -> list[dict]:
        schema = self._schema
        env_n = _norm_env(env)
        stmt = select(schema.derived_minervini).where(
            and_(
                schema.derived_minervini.c.env == env_n,
                schema.derived_minervini.c.as_of == to_date(as_of),
            )
        )
        if symbols:
            stmt = stmt.where(schema.derived_minervini.c.symbol.in_(symbols))
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [self._normalize_derived_row(dict(row)) for row in rows]

    def count_as_of(self, *, env: str, as_of: date) -> int:
        schema = self._schema
        env_n = _norm_env(env)
        stmt = select(func.count()).select_from(schema.derived_minervini).where(
            and_(
                schema.derived_minervini.c.env == env_n,
                schema.derived_minervini.c.as_of == to_date(as_of),
            )
        )
        with self.engine.connect() as conn:
            return int(conn.execute(stmt).scalar() or 0)
    
    def get_latest_as_of(self, *, env: str, requested_as_of: date, ttl_days: int = 7) -> date | None:
        """
        주어진 as_of 이하의 최신 derived as_of를 찾는다.
        
        Args:
            requested_as_of: 요청된 as_of (이 날짜 이하)
            ttl_days: 최대 허용 일수 (기본 7일)
        
        Returns:
            최신 as_of 또는 None (ttl 초과 시)
        
        Examples:
            >>> # 2026-02-10 요청했는데 없으면, 2026-02-09 찾기
            >>> repo.get_latest_as_of(requested_as_of=date(2026, 2, 10))
            date(2026, 2, 9)
        """
        schema = self._schema
        env_n = _norm_env(env)
        requested_date = to_date(requested_as_of)
        
        # 최신 as_of 찾기 (requested_as_of 이하)
        stmt = (
            select(func.max(schema.derived_minervini.c.as_of))
            .where(
                and_(
                    schema.derived_minervini.c.env == env_n,
                    schema.derived_minervini.c.as_of <= requested_date,
                )
            )
        )
        
        with self.engine.connect() as conn:
            latest_as_of = conn.execute(stmt).scalar()
        
        if not latest_as_of:
            logger.warning(
                "[DERIVED][FALLBACK][NOT_FOUND] requested=%s no_data",
                requested_date.isoformat(),
            )
            return None
        
        # TTL 체크
        age_days = (requested_date - latest_as_of).days
        
        if age_days > ttl_days:
            logger.warning(
                "[DERIVED][FALLBACK][TTL_EXCEEDED] requested=%s latest=%s age=%d ttl=%d",
                requested_date.isoformat(),
                latest_as_of.isoformat(),
                age_days,
                ttl_days,
            )
            return None
        
        logger.info(
            "[DERIVED][FALLBACK][OK] requested=%s latest=%s age=%d",
            requested_date.isoformat(),
            latest_as_of.isoformat(),
            age_days,
        )
        
        return latest_as_of
    
    def load_for_as_of_with_fallback(
        self,
        *,
        env: str,
        as_of: date,
        symbols: list[str] | None = None,
        ttl_days: int = 7,
    ) -> tuple[list[dict], date | None]:
        """
        as_of로 derived를 로드하되, 없으면 fallback to 최신 available.
        
        Args:
            as_of: 요청 날짜
            symbols: 종목 필터 (옵션)
            ttl_days: fallback TTL (기본 7일)
        
        Returns:
            (rows, actual_as_of): 로드된 데이터와 실제 사용된 as_of
        
        Examples:
            >>> # 2026-02-10 요청 -> 없으면 2026-02-09로 fallback
            >>> rows, actual = repo.load_for_as_of_with_fallback(
            ...     as_of=date(2026, 2, 10)
            ... )
            >>> actual  # date(2026, 2, 9)
        """
        # 먼저 요청된 as_of로 시도
        rows = self.load_for_as_of(env=env, as_of=as_of, symbols=symbols)
        
        if rows:
            logger.debug(
                "[DERIVED][LOAD][DIRECT] as_of=%s count=%d",
                as_of.isoformat(),
                len(rows),
            )
            return rows, as_of
        
        # 없으면 fallback 시도
        logger.info(
            "[DERIVED][LOAD][FALLBACK_START] requested=%s reason=empty",
            as_of.isoformat(),
        )
        
        fallback_as_of = self.get_latest_as_of(env=env, requested_as_of=as_of, ttl_days=ttl_days)
        
        if not fallback_as_of:
            logger.warning(
                "[DERIVED][LOAD][FALLBACK_FAIL] requested=%s reason=no_valid_fallback",
                as_of.isoformat(),
            )
            return [], None
        
        # fallback으로 재로드
        rows = self.load_for_as_of(env=env, as_of=fallback_as_of, symbols=symbols)
        
        logger.info(
            "[DERIVED][LOAD][FALLBACK_SUCCESS] requested=%s fallback=%s count=%d",
            as_of.isoformat(),
            fallback_as_of.isoformat(),
            len(rows),
        )
        
        return rows, fallback_as_of
    
    def load_derived(
        self,
        *,
        env: str,
        as_of: str | date,
        symbols: list[str] | None = None,
        allow_fallback: bool = False,
        ttl_days: int = 7,
    ) -> list[dict]:
        """
        Minervini derived 데이터를 로드한다.
        
        이 메서드는 watchlist_builder.py와 prep_runner.py에서 호출되는
        호환 API다.
        
        Args:
            env: 환경 (prep, live 등)
            as_of: 요청 날짜 (str 또는 date)
            symbols: 종목 필터 (옵션, None이면 전체)
            allow_fallback: True면 해당 날짜에 없을 때 최근 TTL 내 데이터로 fallback
            ttl_days: fallback 허용 최대 일수
        
        Returns:
            list[dict]: Minervini 점수 row 목록
                각 row는 최소한 다음 필드를 포함:
                - symbol: 종목 코드
                - as_of: 날짜
                - rs_percentile, rs_score, vcp_score, trend_score
                - (가능하면) breakout_score, pullback_score, momentum_score
        
        Examples:
            >>> # 정확한 날짜로만 로드
            >>> rows = repo.load_derived(env="prep", as_of="2026-03-07", allow_fallback=False)
            >>> len(rows)  # 196 또는 0
            
            >>> # fallback 허용
            >>> rows = repo.load_derived(env="prep", as_of="2026-03-08", allow_fallback=True, ttl_days=7)
            >>> # 2026-03-08이 없으면 최근 7일 내 최신 데이터 반환
        """
        as_of_date = to_date(as_of)
        
        if allow_fallback:
            rows, actual_as_of = self.load_for_as_of_with_fallback(
                env=env,
                as_of=as_of_date,
                symbols=symbols,
                ttl_days=ttl_days,
            )
            
            if rows:
                rows = [self._normalize_derived_row(r) for r in rows]
                rs_nonzero = sum(1 for r in rows if (r.get("rs_percentile") or 0) > 0 or (r.get("rs_score") or 0) > 0)
                vcp_nonzero = sum(1 for r in rows if (r.get("vcp_score") or 0) > 0)
                trend_nonzero = sum(1 for r in rows if (r.get("trend_score") or 0) > 0)
                breakout_nonzero = sum(1 for r in rows if (r.get("breakout_score") or 0) > 0)
                pullback_nonzero = sum(1 for r in rows if (r.get("pullback_score") or 0) > 0)
                momentum_nonzero = sum(1 for r in rows if (r.get("momentum_score") or 0) > 0)
                includes_breakout = int(any("breakout_score" in r for r in rows))
                includes_pullback = int(any("pullback_score" in r for r in rows))
                includes_momentum = int(any("momentum_score" in r for r in rows))

                logger.info(
                    "[DERIVED][LOAD][FIELDS] includes_breakout=%d includes_pullback=%d includes_momentum=%d",
                    includes_breakout,
                    includes_pullback,
                    includes_momentum,
                )
                
                logger.info(
                    "[DERIVED][LOAD] env=%s requested_as_of=%s actual_as_of=%s rows=%d fallback=%d rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d",
                    env,
                    as_of_date.isoformat(),
                    actual_as_of.isoformat() if actual_as_of else "None",
                    len(rows),
                    1 if actual_as_of != as_of_date else 0,
                    rs_nonzero,
                    vcp_nonzero,
                    trend_nonzero,
                    breakout_nonzero,
                    pullback_nonzero,
                    momentum_nonzero,
                )
            else:
                logger.warning(
                    "[DERIVED][LOAD][EMPTY] env=%s requested_as_of=%s fallback=%d",
                    env,
                    as_of_date.isoformat(),
                    1,
                )
            
            return rows
        else:
            # Strict mode: 정확한 as_of만 허용
            rows = self.load_for_as_of(env=env, as_of=as_of_date, symbols=symbols)
            
            if rows:
                rows = [self._normalize_derived_row(r) for r in rows]
                rs_nonzero = sum(1 for r in rows if (r.get("rs_percentile") or 0) > 0 or (r.get("rs_score") or 0) > 0)
                vcp_nonzero = sum(1 for r in rows if (r.get("vcp_score") or 0) > 0)
                trend_nonzero = sum(1 for r in rows if (r.get("trend_score") or 0) > 0)
                breakout_nonzero = sum(1 for r in rows if (r.get("breakout_score") or 0) > 0)
                pullback_nonzero = sum(1 for r in rows if (r.get("pullback_score") or 0) > 0)
                momentum_nonzero = sum(1 for r in rows if (r.get("momentum_score") or 0) > 0)
                includes_breakout = int(any("breakout_score" in r for r in rows))
                includes_pullback = int(any("pullback_score" in r for r in rows))
                includes_momentum = int(any("momentum_score" in r for r in rows))

                logger.info(
                    "[DERIVED][LOAD][FIELDS] includes_breakout=%d includes_pullback=%d includes_momentum=%d",
                    includes_breakout,
                    includes_pullback,
                    includes_momentum,
                )
                
                logger.info(
                    "[DERIVED][LOAD] env=%s requested_as_of=%s actual_as_of=%s rows=%d fallback=0 rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d",
                    env,
                    as_of_date.isoformat(),
                    as_of_date.isoformat(),
                    len(rows),
                    rs_nonzero,
                    vcp_nonzero,
                    trend_nonzero,
                    breakout_nonzero,
                    pullback_nonzero,
                    momentum_nonzero,
                )
            else:
                logger.warning(
                    "[DERIVED][LOAD][EMPTY] env=%s requested_as_of=%s fallback=0",
                    env,
                    as_of_date.isoformat(),
                )
            
            return rows
    
    def find_latest_available_asof(
        self,
        *,
        env: str,
        target_as_of: date,
        max_back_days: int = 3,
        ttl_days: int = 7,
    ) -> date | None:
        """
        전일 derived 없을 때 fallback as_of 찾기 (우선순위 탐색).
        
        로직:
        1. target_as_of부터 역순으로 max_back_days까지 순차 조회
           (target, target-1, target-2, ..., target-max_back_days)
        2. 없으면 ttl_days 범위 내에서 가장 최신 as_of 반환
        3. 그것도 없으면 None
        
        Args:
            target_as_of: 목표 as_of (전일)
            max_back_days: 우선 탐색 범위 (기본 3일)
            ttl_days: 최대 허용 일수 (기본 7일)
        
        Returns:
            fallback_as_of | None
        
        Examples:
            >>> # 2026-02-10 요청 -> 없으면 2026-02-09, 2026-02-08, 2026-02-07 순으로 확인
            >>> # 그것도 없으면 ttl_days(7일) 범위 내 최신 반환
            >>> repo.find_latest_available_asof(
            ...     target_as_of=date(2026, 2, 10),
            ...     max_back_days=3,
            ...     ttl_days=7,
            ... )
            date(2026, 2, 9)  # 또는 2026-02-08, 2026-02-07, ...
        """
        schema = self._schema
        env_n = _norm_env(env)
        target_date = to_date(target_as_of)
        
        # Step 1: 우선순위 탐색 (target, target-1, target-2, ..., target-max_back_days)
        for i in range(max_back_days + 1):
            candidate_date = target_date - timedelta(days=i)
            
            stmt = select(func.count()).select_from(schema.derived_minervini).where(
                and_(
                    schema.derived_minervini.c.env == env_n,
                    schema.derived_minervini.c.as_of == candidate_date,
                )
            )
            
            with self.engine.connect() as conn:
                count = int(conn.execute(stmt).scalar() or 0)
            
            if count > 0:
                logger.info(
                    "[DERIVED][FALLBACK][PRIORITY_FOUND] target=%s found=%s offset=%d count=%d",
                    target_date.isoformat(),
                    candidate_date.isoformat(),
                    i,
                    count,
                )
                return candidate_date
        
        # Step 2: 우선순위 탐색 실패 -> ttl 범위 내 최신 검색
        logger.info(
            "[DERIVED][FALLBACK][PRIORITY_MISS] target=%s max_back_days=%d -> trying ttl=%d",
            target_date.isoformat(),
            max_back_days,
            ttl_days,
        )
        
        min_date = target_date - timedelta(days=ttl_days)
        stmt = (
            select(func.max(schema.derived_minervini.c.as_of))
            .where(
                and_(
                    schema.derived_minervini.c.env == env_n,
                    schema.derived_minervini.c.as_of >= min_date,
                    schema.derived_minervini.c.as_of <= target_date,
                )
            )
        )
        
        with self.engine.connect() as conn:
            latest_as_of = conn.execute(stmt).scalar()
        
        if not latest_as_of:
            logger.warning(
                "[DERIVED][FALLBACK][NOT_FOUND] target=%s max_back_days=%d ttl_days=%d",
                target_date.isoformat(),
                max_back_days,
                ttl_days,
            )
            return None
        
        age_days = (target_date - latest_as_of).days
        
        logger.info(
            "[DERIVED][FALLBACK][TTL_FOUND] target=%s fallback=%s age=%d ttl=%d",
            target_date.isoformat(),
            latest_as_of.isoformat(),
            age_days,
            ttl_days,
        )
        
        return latest_as_of


class DerivedFlowRepo:
    """Daily flow feature snapshots used by watchlist/prep diagnostics."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self._schema = schema_for_engine(engine)

    def upsert_rows(self, *, env: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        schema = self._schema
        env_n = _norm_env(env)
        normalized_rows = []
        for row in rows:
            payload = dict(row)
            payload["env"] = env_n
            normalized_rows.append(payload)

        with self.engine.begin() as conn:
            if conn.dialect.name == "postgresql":
                stmt = pg_insert(schema.derived_flow).values(normalized_rows)
                update_cols = {
                    col.name: getattr(stmt.excluded, col.name)
                    for col in schema.derived_flow.c
                    if col.name not in {"env", "as_of", "symbol", "created_at"}
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        schema.derived_flow.c.env,
                        schema.derived_flow.c.as_of,
                        schema.derived_flow.c.symbol,
                    ],
                    set_=update_cols,
                )
                conn.execute(stmt)
            else:
                for payload in normalized_rows:
                    try:
                        conn.execute(sa.insert(schema.derived_flow).values(**payload))
                    except IntegrityError:
                        where_clause = and_(
                            schema.derived_flow.c.env == payload["env"],
                            schema.derived_flow.c.as_of == payload["as_of"],
                            schema.derived_flow.c.symbol == payload["symbol"],
                        )
                        update_cols = {k: v for k, v in payload.items() if k not in {"env", "as_of", "symbol"}}
                        conn.execute(sa.update(schema.derived_flow).where(where_clause).values(**update_cols))
        return len(normalized_rows)

    def count_as_of(self, *, env: str, as_of: date) -> int:
        schema = self._schema
        env_n = _norm_env(env)
        stmt = select(func.count()).select_from(schema.derived_flow).where(
            and_(
                schema.derived_flow.c.env == env_n,
                schema.derived_flow.c.as_of == to_date(as_of),
            )
        )
        with self.engine.connect() as conn:
            return int(conn.execute(stmt).scalar() or 0)

    def load_for_as_of(self, *, env: str, as_of: date, symbols: list[str] | None = None) -> list[dict]:
        schema = self._schema
        env_n = _norm_env(env)
        stmt = select(schema.derived_flow).where(
            and_(
                schema.derived_flow.c.env == env_n,
                schema.derived_flow.c.as_of == to_date(as_of),
            )
        )
        if symbols:
            stmt = stmt.where(schema.derived_flow.c.symbol.in_(symbols))
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(row) for row in rows]


def save_watchlist(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date,
    members: List[Dict[str, Any]],
) -> None:
    """Standalone save_watchlist function."""
    if _norm_strategy(strategy) == "pb1_watchlist_final_scored":
        save_pb1_watchlist_rows(
            engine,
            env=env,
            strategy="pb1_watchlist_final_scored",
            as_of=as_of,
            rows=members,
        )
        return

    save_pb1_watchlist_rows(
        engine,
        env=env,
        strategy=strategy,
        as_of=as_of,
        rows=members,
    )


def load_watchlist(
    engine: Engine,
    *,
    env: str,
    strategy: str,
    as_of: date,
    allow_latest_fallback: bool = True,
    ttl_days: int = 7,
    max_back_days: int = 3,
) -> tuple[List[Dict[str, Any]], date | None]:
    """Standalone load_watchlist function."""
    repo = WatchlistRepo(engine)
    return repo.load_watchlist(
        env=env,
        strategy=strategy,
        as_of=as_of,
        allow_latest_fallback=allow_latest_fallback,
        ttl_days=ttl_days,
        max_back_days=max_back_days,
    )


# ========================================
# OHLCV Helper Functions
# ========================================

def get_ohlcv_last_date(engine: Engine, stock_code: str) -> Optional[date]:
    """
    주어진 종목의 DB에 저장된 OHLCV 마지막 날짜를 조회.
    
    Args:
        engine: DB 엔진
        stock_code: 종목코드
    
    Returns:
        마지막 날짜 또는 None
    """
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        stmt = (
            select(func.max(schema.price_daily.c.date))
            .where(schema.price_daily.c.code == stock_code)
        )
        result = conn.execute(stmt).scalar()
    return result


# ========================================
# Job Checkpoint Functions
# ========================================

def load_job_checkpoint(engine: Engine, job_key: str) -> Optional[Dict[str, Any]]:
    """
    체크포인트 로드.
    
    Args:
        engine: DB 엔진
        job_key: 작업 키
    
    Returns:
        payload dict 또는 None
    """
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        stmt = select(schema.job_checkpoints).where(
            schema.job_checkpoints.c.job_key == job_key
        )
        row = conn.execute(stmt).fetchone()
        if row:
            return dict(row.payload) if row.payload else {}
    return None


def save_job_checkpoint(engine: Engine, job_key: str, payload: Dict[str, Any]) -> None:
    """
    체크포인트 저장 (upsert).
    
    Args:
        engine: DB 엔진
        job_key: 작업 키
        payload: 저장할 상태 정보
    """
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        values = {
            "job_key": job_key,
            "updated_ts": datetime.utcnow(),
            "payload": payload,
        }
        if conn.dialect.name == "postgresql":
            stmt = pg_insert(schema.job_checkpoints).values(**values).on_conflict_do_update(
                index_elements=["job_key"],
                set_={"updated_ts": values["updated_ts"], "payload": values["payload"]}
            )
            conn.execute(stmt)
        else:
            # Fallback for non-PostgreSQL
            try:
                conn.execute(sa.insert(schema.job_checkpoints).values(**values))
            except IntegrityError:
                conn.execute(
                    sa.update(schema.job_checkpoints)
                    .where(schema.job_checkpoints.c.job_key == job_key)
                    .values(updated_ts=values["updated_ts"], payload=values["payload"])
                )


# ========================================
# Institutional Decision Tracking Repos
# ========================================

class WatchlistSnapshotRepo:
    """Final 30 선정 이유 스냅샷 저장/조회."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def save_snapshot(
        self,
        *,
        as_of: date,
        final30: List[Dict[str, Any]],
    ) -> None:
        """
        Final 30 스냅샷 저장.
        
        Args:
            as_of: 기준일
            final30: Final 30 종목 리스트 (각 항목에 code, name, rank, tech_score, flow_score, final_score, reasons 포함)
        """
        as_of = to_date(as_of)
        
        logger.info("[WATCHLIST_SNAPSHOT][SAVE] as_of=%s count=%s", as_of, len(final30))
        
        with self.engine.begin() as conn:
            # 기존 데이터 삭제 (같은 날짜)
            conn.execute(
                sa.text("DELETE FROM watchlist_snapshot WHERE as_of = :as_of"),
                {"as_of": as_of}
            )
            
            # 새 데이터 삽입
            for item in final30:
                conn.execute(
                    sa.text("""
                        INSERT INTO watchlist_snapshot (as_of, code, name, rank, tech_score, flow_score, final_score, reasons)
                        VALUES (:as_of, :code, :name, :rank, :tech_score, :flow_score, :final_score, :reasons::jsonb)
                    """),
                    {
                        "as_of": as_of,
                        "code": item.get("code"),
                        "name": item.get("name"),
                        "rank": item.get("rank"),
                        "tech_score": item.get("tech_score"),
                        "flow_score": item.get("flow_score"),
                        "final_score": item.get("final_score"),
                        "reasons": json_sanitize(item.get("reasons", {})),
                    }
                )
    
    def load_snapshot(self, as_of: date) -> List[Dict[str, Any]]:
        """
        스냅샷 로드.
        
        Args:
            as_of: 기준일
        
        Returns:
            종목 리스트
        """
        as_of = to_date(as_of)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.text("""
                    SELECT code, name, rank, tech_score, flow_score, final_score, reasons
                    FROM watchlist_snapshot
                    WHERE as_of = :as_of
                    ORDER BY rank
                """),
                {"as_of": as_of}
            )
            
            items = []
            for row in result:
                items.append({
                    "code": row[0],
                    "name": row[1],
                    "rank": row[2],
                    "tech_score": row[3],
                    "flow_score": row[4],
                    "final_score": row[5],
                    "reasons": dict(row[6]) if row[6] else {},
                })
            
            return items


class MinerviniSnapshotRepo:
    """미너비니 통과 종목 스냅샷 저장/조회."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def save_snapshot(
        self,
        *,
        as_of: date,
        passed_list: List[Dict[str, Any]],
    ) -> None:
        """
        미너비니 통과 종목 스냅샷 저장.
        
        Args:
            as_of: 기준일
            passed_list: 통과 종목 리스트
        """
        as_of = to_date(as_of)
        
        logger.info("[MINERVINI_SNAPSHOT][SAVE] as_of=%s count=%s", as_of, len(passed_list))
        
        with self.engine.begin() as conn:
            # 기존 데이터 삭제
            conn.execute(
                sa.text("DELETE FROM minervini_snapshot WHERE as_of = :as_of"),
                {"as_of": as_of}
            )
            
            # 새 데이터 삽입
            for item in passed_list:
                conn.execute(
                    sa.text("""
                        INSERT INTO minervini_snapshot (as_of, code, name, rs_percentile, vcp_score, trend_ok, score, reasons)
                        VALUES (:as_of, :code, :name, :rs_percentile, :vcp_score, :trend_ok, :score, :reasons::jsonb)
                    """),
                    {
                        "as_of": as_of,
                        "code": item.get("code"),
                        "name": item.get("name"),
                        "rs_percentile": item.get("rs_percentile"),
                        "vcp_score": item.get("vcp_score"),
                        "trend_ok": item.get("trend_ok"),
                        "score": item.get("score"),
                        "reasons": json_sanitize(item.get("reasons", {})),
                    }
                )
    
    def load_snapshot(self, as_of: date) -> List[Dict[str, Any]]:
        """스냅샷 로드."""
        as_of = to_date(as_of)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.text("""
                    SELECT code, name, rs_percentile, vcp_score, trend_ok, score, reasons
                    FROM minervini_snapshot
                    WHERE as_of = :as_of
                    ORDER BY score DESC
                """),
                {"as_of": as_of}
            )
            
            items = []
            for row in result:
                items.append({
                    "code": row[0],
                    "name": row[1],
                    "rs_percentile": row[2],
                    "vcp_score": row[3],
                    "trend_ok": row[4],
                    "score": row[5],
                    "reasons": dict(row[6]) if row[6] else {},
                })
            
            return items


class EntryDecisionRepo:
    """매수 당시 의사결정 스냅샷 저장/조회."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def save_snapshot(
        self,
        *,
        run_id: str,
        as_of: date,
        code: str,
        entry_price: float,
        stop_price: float,
        qty: int,
        features: Dict[str, Any],
        reasons: Dict[str, Any],
    ) -> int:
        """
        매수 스냅샷 저장.
        
        Args:
            run_id: 실행 ID
            as_of: 매수일
            code: 종목코드
            entry_price: 매수가
            stop_price: 손절가
            qty: 수량
            features: 기술적 특징 스냅샷
            reasons: 매수 이유
        
        Returns:
            생성된 스냅샷 ID
        """
        as_of = to_date(as_of)
        
        logger.info("[ENTRY_SNAPSHOT][SAVE] code=%s run_id=%s", code, run_id)
        
        with self.engine.begin() as conn:
            result = conn.execute(
                sa.text("""
                    INSERT INTO entry_decision_snapshot 
                    (run_id, as_of, code, entry_price, stop_price, qty, features, reasons)
                    VALUES (:run_id, :as_of, :code, :entry_price, :stop_price, :qty, :features::jsonb, :reasons::jsonb)
                    RETURNING id
                """),
                {
                    "run_id": run_id,
                    "as_of": as_of,
                    "code": code,
                    "entry_price": entry_price,
                    "stop_price": stop_price,
                    "qty": qty,
                    "features": json_sanitize(features),
                    "reasons": json_sanitize(reasons),
                }
            )
            
            snapshot_id = result.scalar()
            logger.info("[ENTRY_SNAPSHOT][SAVED] id=%s code=%s", snapshot_id, code)
            return snapshot_id
    
    def load_latest_snapshot(self, code: str) -> Optional[Dict[str, Any]]:
        """
        최근 매수 스냅샷 로드.
        
        Args:
            code: 종목코드
        
        Returns:
            스냅샷 또는 None
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.text("""
                    SELECT id, run_id, as_of, entry_price, stop_price, qty, features, reasons
                    FROM entry_decision_snapshot
                    WHERE code = :code
                    ORDER BY created_at DESC
                    LIMIT 1
                """),
                {"code": code}
            )
            
            row = result.fetchone()
            if row:
                return {
                    "id": row[0],
                    "run_id": row[1],
                    "as_of": row[2],
                    "entry_price": row[3],
                    "stop_price": row[4],
                    "qty": row[5],
                    "features": dict(row[6]) if row[6] else {},
                    "reasons": dict(row[7]) if row[7] else {},
                }
            
            return None


class ExitAnalysisRepo:
    """매도 시점 비교 분석 저장/조회."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def save_analysis(
        self,
        *,
        code: str,
        entry_snapshot_id: int,
        exit_date: date,
        exit_price: float,
        pnl: float,
        pnl_pct: float,
        hold_days: int,
        comparison: Dict[str, Any],
    ) -> int:
        """
        Exit 분석 저장.
        
        Args:
            code: 종목코드
            entry_snapshot_id: 매수 스냅샷 ID
            exit_date: 매도일
            exit_price: 매도가
            pnl: 손익 (원)
            pnl_pct: 손익률
            hold_days: 보유 기간
            comparison: 비교 분석 결과
        
        Returns:
            생성된 분석 ID
        """
        exit_date = to_date(exit_date)
        
        logger.info("[EXIT_ANALYSIS][SAVE] code=%s entry_snapshot_id=%s", code, entry_snapshot_id)
        
        with self.engine.begin() as conn:
            result = conn.execute(
                sa.text("""
                    INSERT INTO exit_analysis_snapshot 
                    (code, entry_snapshot_id, exit_date, exit_price, pnl, pnl_pct, hold_days, comparison)
                    VALUES (:code, :entry_snapshot_id, :exit_date, :exit_price, :pnl, :pnl_pct, :hold_days, :comparison::jsonb)
                    RETURNING id
                """),
                {
                    "code": code,
                    "entry_snapshot_id": entry_snapshot_id,
                    "exit_date": exit_date,
                    "exit_price": exit_price,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                    "hold_days": hold_days,
                    "comparison": json_sanitize(comparison),
                }
            )
            
            analysis_id = result.scalar()
            logger.info("[EXIT_ANALYSIS][SAVED] id=%s code=%s", analysis_id, code)
            return analysis_id
    
    def load_analysis_by_code(self, code: str) -> List[Dict[str, Any]]:
        """종목별 exit 분석 로드."""
        with self.engine.connect() as conn:
            result = conn.execute(
                sa.text("""
                    SELECT id, entry_snapshot_id, exit_date, exit_price, pnl, pnl_pct, hold_days, comparison
                    FROM exit_analysis_snapshot
                    WHERE code = :code
                    ORDER BY exit_date DESC
                """),
                {"code": code}
            )
            
            items = []
            for row in result:
                items.append({
                    "id": row[0],
                    "entry_snapshot_id": row[1],
                    "exit_date": row[2],
                    "exit_price": row[3],
                    "pnl": row[4],
                    "pnl_pct": row[5],
                    "hold_days": row[6],
                    "comparison": dict(row[7]) if row[7] else {},
                })
            
            return items


