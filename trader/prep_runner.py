from __future__ import annotations

import logging
import os
import time
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from settings import RUNTIME_DIR
from trader.db.engine import get_engine
from trader.db.health import assert_db_ready
from trader.db.migrate import run_migrations
from trader.db.repos import (
    CRITICAL_SCORED_COLS,
    REQUIRED_FINAL30_SCORED_COLS,
    LedgerEventsRepo,
    UniverseRepo,
    WatchlistRepo,
)
from trader.minervini.compute import compute_and_store_derived_minervini
from trader.candidate_pool_builder import (
    build_and_save_candidate_pool,
    get_last_candidate_pool_build_report,
)
from trader.watchlist_builder import (
    build_and_save_watchlist,
    recover_bundle_from_db,
    rebuild_bundle,
    save_bundle,
    WatchlistBundle,
    validate_watchlist_contract,
)
from trader.data.ohlcv_provider import (
    compute_required_prefetch_days,
    find_symbols_with_insufficient_history,
    upsert_ohlcv_delta,
)
from trader.exporter import export_watchlist_bundle
from trader.score_columns import (
    collect_nonzero_score_stats,
    has_required_score_fields,
    resolve_score_column,
)
from trader.report.pdf_report import generate_watchlist_pdf
from trader.strategies.pb1_minervini_v2 import MinerviniConfig
from trader.time_utils import (
    calc_market_window_kst,
    is_market_open_kst,
    now_kst,
    prev_business_day,
    resolve_derived_as_of,
)
from trader.runtime_paths import build_final30_scored_paths, get_final30_artifact_paths, repo_root
from trader.utils.json_sanitize import to_jsonable
from trader.universe.build import build_universe

logger = logging.getLogger(__name__)

FINAL30_SCORED_EXPORT_COLS = [
    "code",
    "name",
    "rank_final30",
    "score_final",
    "tech_score",
    "flow_score",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "rs_percentile",
    "vcp_score",
    "atr_pct",
    "pullback_pct",
    "entry_style_selected",
]

def _env_true(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _resolve_flow_as_of(*, requested_as_of: date, now_ts: datetime | None = None) -> date:
    now_ts = now_ts or now_kst()
    trade_date = now_ts.date()
    if is_market_open_kst(now_ts) or requested_as_of == trade_date:
        return prev_business_day(trade_date)
    return requested_as_of


def _make_flow_provider(engine):
    # ✅ FIX: KIS 우선으로 변경 (pykrx JSONDecodeError 방지)
    providers = ["kis", "pykrx"]
    flow_cache: dict[tuple[str, str, int], tuple[pd.DataFrame | None, pd.DataFrame | None, str | None]] = {}
    kis_api = None
    kis_init_failed = False

    def _resolve_code_from_df(df: pd.DataFrame, code: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        code_candidates = {code, f"A{code}", code.lstrip("A")}
        work = df.copy()
        work.index = work.index.astype(str)
        matched_idx = next((idx for idx in work.index if idx in code_candidates), None)
        if matched_idx is None:
            return pd.DataFrame()
        row = work.loc[[matched_idx]]
        return row

    def _extract_net_buy(row_df: pd.DataFrame) -> float | None:
        if row_df is None or row_df.empty:
            return None
        candidate_cols = [
            "순매수수량",
            "순매수거래량",
            "순매수수량(주)",
            "순매수거래대금",
            "순매수대금",
            "net_buy",
        ]
        for col in candidate_cols:
            if col not in row_df.columns:
                continue
            try:
                val = row_df.iloc[0][col]
                if isinstance(val, str):
                    val = val.replace(",", "").strip()
                return float(val)
            except Exception:
                continue
        return None

    def _provider_pykrx(code: str, flow_as_of: date, _window: int) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        try:
            from pykrx import stock
        except Exception as exc:
            logger.warning("[FLOW][WARN] pykrx import failed err=%s", exc)
            return None, None

        ymd = flow_as_of.strftime("%Y%m%d")
        try:
            fr_all = stock.get_market_net_purchases_of_equities_by_ticker(ymd, ymd, "ALL", "외국인")
            inst_all = stock.get_market_net_purchases_of_equities_by_ticker(ymd, ymd, "ALL", "기관합계")
            fr_row = _resolve_code_from_df(fr_all, code)
            inst_row = _resolve_code_from_df(inst_all, code)
            fr_net = _extract_net_buy(fr_row)
            inst_net = _extract_net_buy(inst_row)
            if fr_net is None or inst_net is None:
                return None, None
            foreign_df = pd.DataFrame([{"date": flow_as_of, "net_buy": fr_net}])
            inst_df = pd.DataFrame([{"date": flow_as_of, "net_buy": inst_net}])
            return foreign_df, inst_df
        except Exception as exc:
            logger.warning("[FLOW][WARN] pykrx fetch failed code=%s as_of=%s err=%s", code, flow_as_of, exc)
            return None, None

    def _provider_kis(code: str, flow_as_of: date, _window: int) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        nonlocal kis_api, kis_init_failed
        if kis_init_failed:
            return None, None
        if kis_api is None:
            try:
                from trader.kis_wrapper import KisAPI

                kis_api = KisAPI(kis_env=os.getenv("KIS_ENV", "practice"))
            except Exception as exc:
                kis_init_failed = True
                logger.warning("[FLOW][WARN] KIS init failed err=%s", exc)
                return None, None

        try:
            resp = kis_api.inquire_investor(code, "KOSDAQ")
            if not resp.get("ok"):
                return None, None
            inv = resp.get("inv") or {}
            foreign_raw = inv.get("frgn_ntby_qty", inv.get("frgn_ntby_tr_pbmn"))
            inst_raw = inv.get("orgn_ntby_qty", inv.get("orgn_ntby_tr_pbmn"))
            foreign_net = float(foreign_raw) if foreign_raw is not None else None
            inst_net = float(inst_raw) if inst_raw is not None else None
            if foreign_net is None or inst_net is None:
                return None, None
            foreign_df = pd.DataFrame([{"date": flow_as_of, "net_buy": foreign_net}])
            inst_df = pd.DataFrame([{"date": flow_as_of, "net_buy": inst_net}])
            return foreign_df, inst_df
        except Exception as exc:
            logger.warning("[FLOW][WARN] KIS fetch failed code=%s as_of=%s err=%s", code, flow_as_of, exc)
            return None, None

    provider_map = {
        "pykrx": _provider_pykrx,
        "kis": _provider_kis,
    }

    logger.info("[FLOW][SOURCE] providers=%s priority=KIS→pykrx(fallback)", providers)

    def _provider(code: str, as_of: date, window: int):
        flow_as_of = _resolve_flow_as_of(requested_as_of=as_of)
        cache_key = (str(code).zfill(6), flow_as_of.isoformat(), int(window))
        cached = flow_cache.get(cache_key)
        if cached is not None:
            return cached[0], cached[1]

        for provider_name in providers:
            provider = provider_map.get(provider_name)
            if provider is None:
                continue
            foreign_df, inst_df = provider(str(code).zfill(6), flow_as_of, int(window))
            has_foreign = foreign_df is not None and not foreign_df.empty
            has_inst = inst_df is not None and not inst_df.empty
            if has_foreign and has_inst:
                logger.info(
                    "[FLOW][SOURCE] symbol=%s requested_as_of=%s flow_as_of=%s provider=%s",
                    str(code).zfill(6),
                    as_of,
                    flow_as_of,
                    provider_name,
                )
                flow_cache[cache_key] = (foreign_df, inst_df, provider_name)
                return foreign_df, inst_df

        logger.warning(
            "[FLOW][WARN] all providers failed symbol=%s requested_as_of=%s flow_as_of=%s providers=%s",
            str(code).zfill(6),
            as_of,
            flow_as_of,
            providers,
        )
        flow_cache[cache_key] = (None, None, None)
        return None, None

    return _provider


def _pick_as_of_date_always_prev() -> date:
    """PREP as_of 결정: AS_OF_OVERRIDE 우선, 없으면 전 거래일."""
    return resolve_derived_as_of(now_kst())


def get_required_history_days(strategy_name: str) -> int:
    """Centralized long-lookback requirement for PREP OHLCV readiness."""
    _ = strategy_name
    ma150_days = 150
    rs_lookback_126 = 126
    vcp_lookback_120 = 120
    atr_days_20 = 20
    safety_margin = 30
    return max(ma150_days, rs_lookback_126, vcp_lookback_120, atr_days_20) + safety_margin


def _collect_final30_nonzero_stats(df: pd.DataFrame) -> tuple[dict[str, int], dict[str, str | None]]:
    stats, alias_cols = collect_nonzero_score_stats(df)
    for key in (
        "tech_nonzero",
        "final_nonzero",
        "score_final_nonzero",
        "breakout_nonzero",
        "pullback_nonzero",
        "momentum_nonzero",
        "rs_nonzero",
        "vcp_nonzero",
        "trend_nonzero",
    ):
        stats.setdefault(key, 0)
    return stats, alias_cols


def _has_required_final30_score_fields(df: pd.DataFrame) -> bool:
    return has_required_score_fields(df, ("tech", "final", "breakout", "pullback", "momentum"))


def _extract_container_value(container: Any, key: str) -> Any:
    if container is None:
        return None
    if isinstance(container, dict):
        return container.get(key)
    if hasattr(container, key):
        return getattr(container, key)
    return None


def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _as_dataframe(value: Any) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if isinstance(value, list):
        return pd.DataFrame(value)
    if isinstance(value, tuple):
        return pd.DataFrame(list(value))
    if isinstance(value, dict):
        return pd.DataFrame([value])
    return pd.DataFrame()


def _build_scored_members(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    critical_missing_from_source = [col for col in CRITICAL_SCORED_COLS if col not in df.columns]
    if critical_missing_from_source:
        raise RuntimeError(
            f"[PREP][FINAL30_SCORED][INTEGRITY_FAIL] source_missing_critical_cols={critical_missing_from_source}"
        )

    normalized_df = df.copy(deep=True)
    for col in REQUIRED_FINAL30_SCORED_COLS:
        if col not in normalized_df.columns:
            normalized_df[col] = None
    normalized_df = normalized_df[REQUIRED_FINAL30_SCORED_COLS]

    missing_after_normalize = [col for col in CRITICAL_SCORED_COLS if col not in normalized_df.columns]
    if missing_after_normalize:
        raise RuntimeError(
            f"[PREP][FINAL30_SCORED][INTEGRITY_FAIL] normalized_missing_critical_cols={missing_after_normalize}"
        )

    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(normalized_df.to_dict(orient="records"), start=1):
        code = str(row.get("code") or "").zfill(6)
        if not code:
            continue
        payload = dict(row)
        payload["code"] = code
        payload["rank"] = int(payload.get("rank") or payload.get("rank_final30") or idx)
        payload["as_of"] = payload.get("as_of")

        score_final = payload.get("score")
        if score_final is None:
            score_final = payload.get("score_final")
        if score_final is None:
            score_final = payload.get("final_score")
        rows.append(
            {
                "code": code,
                "rank": int(payload.get("rank") or idx),
                "score": float(score_final) if score_final is not None else None,
                "meta": payload,
                **payload,
            }
        )
    return rows


def _write_canonical_final30_scored_files(*, env: str, as_of: str, df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    payload_df = (df.copy() if df is not None else pd.DataFrame())
    for col in FINAL30_SCORED_EXPORT_COLS:
        if col not in payload_df.columns:
            payload_df[col] = None
    payload_df = payload_df[FINAL30_SCORED_EXPORT_COLS]
    payload_df["code"] = payload_df["code"].astype(str).str.zfill(6)
    payload = payload_df.to_dict(orient="records")
    critical_cols = [col for col in CRITICAL_SCORED_COLS if col in payload_df.columns]
    file_results: dict[str, dict[str, Any]] = {}

    def _write_payload(label: str, path: Path) -> None:
        if label == "signals":
            file_payload = {
                "as_of": as_of,
                "env": env,
                "count": len(payload),
                "items": payload,
                "source": "prep_final30_scored",
            }
        else:
            file_payload = payload
        tmp_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
        tmp_path.write_text(json.dumps(file_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, path)

    def _validate_saved_file(label: str, path: Path) -> bool:
        exists = int(path.exists())
        bytes_written = 0
        json_ok = 0
        rows: list[dict[str, Any]] = []
        if exists:
            try:
                bytes_written = int(path.stat().st_size)
            except OSError:
                bytes_written = 0
            try:
                parsed = json.loads(path.read_text(encoding="utf-8"))
                json_ok = 1
                if isinstance(parsed, dict) and isinstance(parsed.get("items"), list):
                    rows = [dict(item) for item in parsed.get("items") if isinstance(item, dict)]
                elif isinstance(parsed, list):
                    rows = [dict(item) for item in parsed if isinstance(item, dict)]
            except Exception:
                json_ok = 0
        detected_critical_cols = sorted([col for col in CRITICAL_SCORED_COLS if rows and col in rows[0]])
        file_results[label] = {
            "path": path,
            "exists": bool(exists == 1),
            "bytes": bytes_written,
            "rows": len(rows),
            "json_ok": bool(json_ok == 1),
            "critical_cols": detected_critical_cols,
        }
        logger.info(
            "[PREP][FINAL30_FILE][WRITE] label=%s path=%s exists=%s bytes=%s rows=%s",
            label,
            str(path),
            path.exists(),
            bytes_written,
            len(rows),
        )
        logger.info(
            "[PREP][FINAL30_SCORED][VERIFY] source=%s path=%s exists=%s bytes=%s json_ok=%s rows=%s critical_cols=%s",
            label,
            path,
            exists,
            bytes_written,
            json_ok,
            len(rows),
            detected_critical_cols,
        )
        return bool(
            exists == 1
            and bytes_written > 0
            and json_ok == 1
            and len(rows) == 30
            and set(CRITICAL_SCORED_COLS).issubset(set(detected_critical_cols))
        )

    for label, path in build_final30_scored_paths(repo_root(), env, as_of).items():
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_payload(label, path)
        if not _validate_saved_file(label, path):
            logger.warning("[PREP][FINAL30_SCORED][RETRY] source=%s path=%s", label, path)
            _write_payload(label, path)
            if not _validate_saved_file(label, path):
                logger.warning(
                    "[PREP][FINAL30_SCORED][VERIFY_FAIL] source=%s path=%s expected_rows=30 required_cols=%s",
                    label,
                    path,
                    critical_cols,
                )

    logger.info(
        "[PREP][FINAL30_SCORED][FIELDS] has_score_final=%s has_tech_score=%s has_breakout_score=%s has_pullback_score=%s has_momentum_score=%s",
        int("score_final" in payload_df.columns),
        int("tech_score" in payload_df.columns),
        int("breakout_score" in payload_df.columns),
        int("pullback_score" in payload_df.columns),
        int("momentum_score" in payload_df.columns),
    )
    return file_results


def _is_scored_final30_df(df: pd.DataFrame) -> bool:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return False
    skinny_cols = {"code", "meta", "rank", "score"}
    normalized_cols = {str(col) for col in df.columns}
    if normalized_cols.issubset(skinny_cols):
        return False
    matched = 0
    for logical_name in ("tech", "final", "breakout", "pullback", "momentum"):
        if resolve_score_column(df, logical_name) is not None:
            matched += 1
    return matched >= 2


def _is_valid_final30_scored_df(df: Any) -> bool:
    if df is None:
        return False
    if not isinstance(df, pd.DataFrame):
        return False
    if getattr(df, "empty", True):
        return False
    if not hasattr(df, "columns"):
        return False
    cols = set(df.columns)
    has_code = "code" in cols
    has_tech = "tech_score" in cols
    has_final = ("score_final" in cols) or ("final_score" in cols)
    has_entry = (
        ("breakout_score" in cols)
        or ("pullback_score" in cols)
        or ("momentum_score" in cols)
    )
    return has_code and has_tech and has_final and has_entry


def _select_final30_scored_df_for_export(
    *,
    watchlist_result: Any,
    watchlist_bundle: Any,
    bundle_final30_scored_before_save_df: pd.DataFrame,
    final30_saved_df: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    candidates: list[tuple[str, Any]] = [
        ("watchlist_result.final30_scored", _safe_get(watchlist_result, "final30_scored")),
        ("watchlist_bundle.final30_scored", _safe_get(watchlist_bundle, "final30_scored")),
        (
            "watchlist_result.bundle_final30_scored_before_save",
            _safe_get(watchlist_result, "bundle_final30_scored_before_save"),
        ),
        (
            "watchlist_bundle.bundle_final30_scored_before_save",
            _safe_get(watchlist_bundle, "bundle_final30_scored_before_save"),
        ),
        ("bundle_final30_scored_before_save", bundle_final30_scored_before_save_df),
    ]

    details: list[dict[str, Any]] = []
    for label, candidate in candidates:
        candidate_df = _as_dataframe(candidate)
        is_valid = _is_valid_final30_scored_df(candidate_df)
        if candidate_df is None or candidate_df.empty:
            details.append({"label": label, "state": "none"})
            continue
        details.append(
            {
                "label": label,
                "state": "present",
                "rows": int(len(candidate_df)) if hasattr(candidate_df, "__len__") else None,
                "cols": list(candidate_df.columns) if hasattr(candidate_df, "columns") else None,
                "valid": is_valid,
            }
        )
        if _is_valid_final30_scored_df(candidate_df):
            logger.info(
                "[PREP][EXPORT][FINAL30][SOURCE] label=%s rows=%s cols=%s",
                label,
                int(len(candidate_df)),
                list(candidate_df.columns),
            )
            return candidate_df.copy(deep=True), label
    if final30_saved_df is not None and not final30_saved_df.empty:
        details.append(
            {
                "label": "watchlist_bundle.final30_saved",
                "state": "present",
                "rows": int(len(final30_saved_df)),
                "cols": list(final30_saved_df.columns),
            }
        )
    logger.error(
        "[PREP][EXPORT][FINAL30][SOURCE][FAIL] no_valid_scored_source details=%s",
        details,
    )
    raise RuntimeError("final30_scored_source_missing")


def _debug_compare_nonzero(
    *,
    key: str,
    inmem: int,
    exported: int,
    lhs_source: str,
    rhs_source: str,
    lhs_col: str | None,
    rhs_col: str | None,
) -> None:
    logger.info(
        "[PREP][EXPORT][COMPARE] key=%s inmem=%s exported=%s lhs_source=%s rhs_source=%s",
        key,
        inmem,
        exported,
        lhs_source,
        rhs_source,
    )
    logger.info(
        "[PREP][EXPORT][COMPARE][FIELDS] key=%s lhs_col=%s rhs_col=%s",
        key,
        lhs_col or "",
        rhs_col or "",
    )


def _assert_same_nonzero(stage: str, inmem: int, exported: int) -> None:
    if int(inmem) != int(exported):
        raise RuntimeError(
            f"{stage}_SCORE_MISMATCH: inmem={inmem} exported={exported}"
        )


def _handle_export_consistency_failure(*, strict_export: bool) -> None:
    if strict_export:
        raise RuntimeError("PREP_EXPORT_FINAL30_CONSISTENCY_FAILED")
    logger.warning(
        "[PREP][EXPORT][NON_STRICT] consistency_failed_but_continue required_states=derived+final30+prep_done",
    )


def _as_of_today() -> date:
    """Deprecated: 전일 고정을 위해 _pick_as_of_date_always_prev() 사용"""
    return now_kst().date()


def _load_db_ohlcv_df(*, engine, code: str, as_of: date, count: int) -> pd.DataFrame:
    from trader.db.repos import load_price_daily

    start_date = as_of - timedelta(days=count * 2)
    candles = load_price_daily(engine, code, start_date, as_of)
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def _ensure_universe(*, engine, env: str, strategy: str, as_of: date) -> list[dict]:
    """
    Ensure universe exists in DB for (env, strategy, as_of) with strict as_of semantics.
    - Load with fallback disabled.
    - If missing (or force always), build and persist for exact as_of.
    - Reload exact as_of and fail hard when still empty.
    """
    repo = UniverseRepo(engine)
    as_of_s = as_of.isoformat()

    no_fallback = os.getenv("UNIVERSE_NO_FALLBACK", "1") == "1"
    build_if_missing = os.getenv("UNIVERSE_BUILD_IF_MISSING", "1") == "1"
    force_always = os.getenv("UNIVERSE_FORCE_REBUILD_ALWAYS", "0") == "1"

    # 1) strict as_of load (no fallback)
    members = repo.get_universe_members(
        env=env,
        strategy=strategy,
        as_of_date=as_of_s,
        allow_fallback=False,
    )

    # 2) build trigger
    should_build = force_always or len(members) == 0
    if should_build:
        if len(members) == 0 and not build_if_missing:
            raise RuntimeError(
                f"Universe missing and auto-build disabled: env={env} strategy={strategy} as_of={as_of_s}"
            )

        reason = "force_always" if force_always else "missing_as_of"
        logger.warning(
            "[UNIVERSE][AUTO_BUILD][RUN] env=%s strategy=%s as_of=%s reason=%s no_fallback=%s build_if_missing=%s",
            env,
            strategy,
            as_of_s,
            reason,
            no_fallback,
            build_if_missing,
        )
        built = build_universe(as_of_date=as_of_s, env=env, strategy=strategy)
        logger.info("[UNIVERSE][DB][UPSERT/SAVE] env=%s strategy=%s as_of=%s members=%d", env, strategy, as_of_s, len(built))
        logger.info("[UNIVERSE][AUTO_BUILD][DONE] as_of=%s members=%d", as_of_s, len(built))
        logger.info(
            "[UNIVERSE][SNAPSHOT_META] requested_as_of=%s actual_as_of=%s source=%s build_reason=%s universe_name=%s member_count=%s created_at=%s",
            as_of_s,
            as_of_s,
            "build_universe",
            reason,
            strategy,
            len(built),
            now_kst().isoformat(),
        )

        # 3) strict reload verification (exact as_of only)
        members = repo.get_universe_members(
            env=env,
            strategy=strategy,
            as_of_date=as_of_s,
            allow_fallback=False,
        )

    # 4) hard fail if exact as_of still empty
    if len(members) == 0:
        raise RuntimeError(
            f"Universe build/save failed: as_of universe is empty after ensure/build: "
            f"env={env} strategy={strategy} as_of={as_of_s}"
        )

    logger.info(
        "[UNIVERSE][SNAPSHOT_META] requested_as_of=%s actual_as_of=%s source=%s build_reason=%s universe_name=%s member_count=%s created_at=%s",
        as_of_s,
        as_of_s,
        "db_exact",
        "exact_load",
        strategy,
        len(members),
        now_kst().isoformat(),
    )

    return members


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    assert_db_ready()
    engine = get_engine()
    run_migrations(engine)

    env = os.getenv("STRATEGY_ENV", "practice").lower()
    mode = os.getenv("MODE", "prep").strip().lower()
    universe_strategy = os.getenv("CANDIDATE_POOL_UNIVERSE_STRATEGY", "best_k_meta")
    run_ts = now_kst()
    run_date = run_ts.date()
    market_window = calc_market_window_kst(run_ts)
    effective_as_of = _pick_as_of_date_always_prev()
    as_of = effective_as_of
    degraded_exclude_flow = _env_true("DEGRADED_EXCLUDE_FLOW", "1")
    allow_degraded_prep = _env_true("ALLOW_DEGRADED_PREP", "0")
    allow_flow_degraded_prep = _env_true("ALLOW_FLOW_DEGRADED_PREP", "1") or allow_degraded_prep
    
    # ✅ FIX: Define pool_min, topk, finaln at the top to avoid NameError
    pool_min = int(os.getenv("PB1_WATCHLIST_POOL_MIN", "40"))
    topk = int(os.getenv("PB1_WATCHLIST_TOPK", "50"))
    finaln = int(os.getenv("PB1_WATCHLIST_FINALN", "30"))

    as_of_reason = "AS_OF_OVERRIDE" if (os.getenv("AS_OF_OVERRIDE") or "").strip() else "PREV_TRADING_DAY"
    logger.info("[PREP][START] env=%s as_of=%s (%s)", env, effective_as_of, as_of_reason)
    logger.info(
        "[PREP][DATE_POLICY] run_date=%s window=%s as_of=%s reason=%s",
        run_date.isoformat(),
        market_window,
        effective_as_of.isoformat(),
        as_of_reason,
    )
    logger.info(
        "[PREP][TODAY_POLICY] run_date=%s market_window=%s as_of=%s final30_for_trade_date=%s",
        run_date.isoformat(),
        market_window,
        effective_as_of.isoformat(),
        run_date.isoformat(),
    )
    logger.info("[PREP][POLICY] contract_mode=degrade_allowed")
    logger.info("[PREP][POLICY] verify_mode=degrade_allowed")
    t0 = time.monotonic()

    stage_as_of: dict[str, str] = {
        "universe": effective_as_of.isoformat(),
        "ohlcv": effective_as_of.isoformat(),
        "derived": effective_as_of.isoformat(),
        "candidate_pool": effective_as_of.isoformat(),
        "watchlist": effective_as_of.isoformat(),
    }

    members = _ensure_universe(engine=engine, env=env, strategy=universe_strategy, as_of=effective_as_of)
    if not members:
        logger.error("[PREP][FAIL] universe empty")
        return 1

    # ---- RS benchmark handling (229200 etc.) ----
    bench = os.getenv("RS_BENCHMARK", "229200").strip()

    # Universe symbols (strict str list for downstream typed functions)
    symbols: list[str] = []
    for member in members:
        code = member.get("code")
        if isinstance(code, str):
            code_norm = code.strip()
            if code_norm:
                symbols.append(code_norm)

    # Ensure benchmark included for downstream RS/Stage_B computations
    if bench and bench not in symbols:
        symbols.append(bench)

    t_ohlcv = time.monotonic()
    logger.info("[PREP][HEARTBEAT] stage=ohlcv_prefetch status=start")
    prefetch_mode = os.getenv("OHLCV_PREFETCH_MODE", "auto").strip().lower()
    prefetch_days = int(os.getenv("PREFETCH_DAYS", "5"))
    delta_days = max(1, int(os.getenv("OHLCV_DELTA_DAYS", "5")))
    auto_min_history_days = max(1, int(os.getenv("OHLCV_AUTO_MIN_HISTORY_DAYS", "150")))
    vcp_lookback = int(os.getenv("MINERVINI_BASE_LOOKBACK_MAX", "80"))
    rs_lookbacks = [
        int(os.getenv("RS_LOOKBACK_DAYS", "63")),
        int(os.getenv("RS_LOOKBACK2_DAYS", "126")),
    ]
    need_days = compute_required_prefetch_days(
        prefetch_days=prefetch_days,
        vcp_lookback=vcp_lookback,
        rs_lookbacks=rs_lookbacks,
    )

    logger.info(
        "[PREP][PHASE] ohlcv_prefetch_start symbols=%s mode=%s prefetch_days=%s delta_days=%s need_days=%s benchmark=%s",
        len(symbols),
        prefetch_mode,
        prefetch_days,
        delta_days,
        need_days,
        bench,
    )

    if prefetch_mode not in {"auto", "full", "delta"}:
        logger.warning("[PREP][OHLCV][MODE_INVALID] mode=%s -> fallback=auto", prefetch_mode)
        prefetch_mode = "auto"

    delta_result = {"symbols": 0, "inserted": 0, "updated": 0, "failed": 0, "failed_symbols": [], "dates": []}
    full_result = {"symbols": 0, "inserted": 0, "updated": 0, "failed": 0, "failed_symbols": [], "dates": []}

    if prefetch_mode == "delta":
        delta_result = upsert_ohlcv_delta(symbols=symbols, as_of=effective_as_of, days=delta_days)
    elif prefetch_mode == "full":
        full_result = upsert_ohlcv_delta(symbols=symbols, as_of=effective_as_of, days=int(need_days))
    else:
        delta_result = upsert_ohlcv_delta(symbols=symbols, as_of=effective_as_of, days=delta_days)
        required_history_days = get_required_history_days("pb1/minervini")
        insufficient_symbols, _ = find_symbols_with_insufficient_history(
            symbols=symbols,
            as_of=effective_as_of,
            min_history_days=required_history_days,
        )
        logger.info(
            "[PREP][OHLCV][READINESS] required_days=%d ready=%d insufficient=%d total=%d",
            required_history_days,
            len(symbols) - len(insufficient_symbols),
            len(insufficient_symbols),
            len(symbols),
        )
        logger.info(
            "[PREP][OHLCV][AUTO_CHECK] min_history_days=%s insufficient=%s/%s",
            auto_min_history_days,
            len(insufficient_symbols),
            len(symbols),
        )
        if insufficient_symbols:
            sample = sorted(insufficient_symbols)[:10]
            logger.warning(
                "[PREP][OHLCV][AUTO_BACKFILL] symbols=%s need_days=%s sample=%s",
                len(insufficient_symbols),
                need_days,
                sample,
            )
            full_result = upsert_ohlcv_delta(symbols=insufficient_symbols, as_of=effective_as_of, days=int(need_days))
        else:
            logger.info("[PREP][OHLCV][AUTO_BACKFILL] skipped (all symbols have enough history)")

    logger.info(
        "[OHLCV][PREFETCH_DONE] mode=%s benchmark=%s delta=%s full=%s",
        prefetch_mode,
        bench,
        to_jsonable(delta_result),
        to_jsonable(full_result),
    )

    dt_ohlcv = time.monotonic() - t_ohlcv
    logger.info("[PREP][HEARTBEAT] stage=ohlcv_prefetch status=done")
    logger.info("[STAGE][DONE] name=%s dt=%.2f", "ohlcv_prefetch", dt_ohlcv)

    t_derived = time.monotonic()
    logger.info("[PREP][HEARTBEAT] stage=derived_minervini status=start")
    derived_upserted = compute_and_store_derived_minervini(
        engine=engine,
        symbols=symbols,
        env=env,
        as_of=effective_as_of,
        lookback_days=int(os.getenv("MINERVINI_OHLCV_DAYS", "520")),
    )
    dt_derived = time.monotonic() - t_derived
    logger.info("[PREP][HEARTBEAT] stage=derived_minervini status=done")
    logger.info("[STAGE][DONE] name=%s dt=%.2f", "derived_minervini", dt_derived)

    # ✅ VERIFY: Check derived_minervini scores immediately after computation
    logger.info("[PREP][DERIVED][MINERVINI] upserted=%s", derived_upserted)
    
    # Derived verification with FAIL on quality issues
    derived_verify_passed = False
    derived_verify_reason = ""
    
    try:
        from trader.db.repos import DerivedMinerviniRepo
        minervini_repo = DerivedMinerviniRepo(engine)
        verify_rows = minervini_repo.load_derived(env=env, as_of=effective_as_of, allow_fallback=False)
        
        if not verify_rows:
            derived_verify_reason = "no_rows_loaded"
            logger.error(
                "[PREP][DERIVED_VERIFY][FAIL] as_of=%s rows=0 reason=%s",
                effective_as_of,
                derived_verify_reason,
            )
        else:
            row_count = len(verify_rows)
            sample_rows = [
                {
                    "symbol": r.get("symbol"),
                    "close": r.get("close"),
                    "pivot_price": r.get("pivot_price"),
                    "breakout_score": r.get("breakout_score"),
                    "pullback_score": r.get("pullback_score"),
                    "momentum_score": r.get("momentum_score"),
                }
                for r in verify_rows[:5]
            ]
            logger.info("[DERIVED][MINERVINI][RAW_SAMPLE] %s", sample_rows)
            expected_min_rows = int(len(symbols) * 0.80)  # 80% of universe
            
            rs_nonzero = sum(1 for r in verify_rows if float(r.get("rs_percentile", 0) or 0) > 0 or float(r.get("rs_score", 0) or 0) > 0)
            vcp_nonzero = sum(1 for r in verify_rows if float(r.get("vcp_score", 0) or 0) > 0)
            trend_nonzero = sum(1 for r in verify_rows if float(r.get("trend_score", 0) or 0) > 0)
            breakout_nonzero = sum(1 for r in verify_rows if float(r.get("breakout_score", 0) or 0) > 0)
            pullback_nonzero = sum(1 for r in verify_rows if float(r.get("pullback_score", 0) or 0) > 0)
            momentum_nonzero = sum(1 for r in verify_rows if float(r.get("momentum_score", 0) or 0) > 0)
            entry_scores_deferred_to_watchlist = 1 if (
                breakout_nonzero == 0 and pullback_nonzero == 0 and momentum_nonzero == 0
            ) else 0
            
            # Quality checks
            quality_failures = []
            
            if row_count < expected_min_rows:
                quality_failures.append(f"row_count_too_low:{row_count}<{expected_min_rows}")
            
            if rs_nonzero == 0 and vcp_nonzero == 0 and trend_nonzero == 0:
                quality_failures.append("all_minervini_scores_zero")
            
            if quality_failures:
                derived_verify_passed = False
                derived_verify_reason = ";".join(quality_failures)
                logger.error(
                    "[PREP][DERIVED_VERIFY][FAIL] as_of=%s rows=%d rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d reason=%s",
                    effective_as_of,
                    row_count,
                    rs_nonzero,
                    vcp_nonzero,
                    trend_nonzero,
                    breakout_nonzero,
                    pullback_nonzero,
                    momentum_nonzero,
                    derived_verify_reason,
                )
            else:
                derived_verify_passed = True
                logger.info(
                    "[PREP][DERIVED_VERIFY][OK] as_of=%s rows=%d rs_nonzero=%d vcp_nonzero=%d trend_nonzero=%d breakout_nonzero=%d pullback_nonzero=%d momentum_nonzero=%d entry_scores_deferred_to_watchlist=%d",
                    effective_as_of,
                    row_count,
                    rs_nonzero,
                    vcp_nonzero,
                    trend_nonzero,
                    breakout_nonzero,
                    pullback_nonzero,
                    momentum_nonzero,
                    entry_scores_deferred_to_watchlist,
                )
    except Exception as e:
        derived_verify_passed = False
        derived_verify_reason = f"exception:{str(e)}"
        logger.error("[PREP][DERIVED_VERIFY][ERROR] verification failed: %s", str(e), exc_info=True)
    
    # FAIL PREP if derived verification failed (unless explicitly disabled)
    if not derived_verify_passed and not _env_true("SKIP_DERIVED_VERIFY", "0"):
        error_msg = f"PREP FAILED: derived_minervini verification failed - {derived_verify_reason}"
        logger.error("[PREP][FATAL] %s", error_msg)
        raise RuntimeError(error_msg)

    t_pool = time.monotonic()
    logger.info("[PREP][HEARTBEAT] stage=candidate_pool status=start")
    force_candidate = os.getenv("FORCE_CANDIDATE", "0") == "1"
    watchlist_force_rebuild = (
        force_candidate
        or _env_true("WATCHLIST_FORCE_REBUILD", "0")
        or _env_true("PB1_WATCHLIST_FORCE_REBUILD", "0")
        or mode == "minervini_test"
    )

    def _pool_ohlcv(code: str, days: int = 80):
        return _load_db_ohlcv_df(engine=engine, code=code, as_of=as_of, count=days)

    try:
        pool_codes = build_and_save_candidate_pool(
            engine=engine,
            env=env,
            as_of=effective_as_of,
            members=members,
            ohlcv_provider=_pool_ohlcv,
            force_rebuild=force_candidate,
            skip_prefetch=True,
        )
    except Exception as exc:
        report = get_last_candidate_pool_build_report()
        logger.error(
            "[PREP][CANDIDATE_POOL][FAIL] reason=%s strict_kept=%s relaxed_kept=%s fallback_eligible=%s final_selected=%s min_size=%s thresholds=%s",
            str(exc),
            report.get("strict_kept", "na"),
            report.get("relaxed_kept", "na"),
            report.get("fallback_eligible", "na"),
            report.get("final_selected", "na"),
            report.get("min_size", "na"),
            report.get("thresholds_used", {}),
        )
        raise

    pool_report = get_last_candidate_pool_build_report()
    mode_counts = pool_report.get("selection_mode_counts", {}) if isinstance(pool_report, dict) else {}
    logger.info(
        "[PREP][CANDIDATE_POOL] selected=%s source=builder strict=%s relaxed=%s fallback=%s",
        len(pool_codes),
        int(mode_counts.get("strict_minervini", 0)),
        int(mode_counts.get("relaxed_minervini", 0)),
        int(mode_counts.get("fallback_topup", 0)),
    )
    dt_pool = time.monotonic() - t_pool
    logger.info("[PREP][HEARTBEAT] stage=candidate_pool status=done")
    logger.info("[STAGE][DONE] name=%s dt=%.2f", "candidate_pool", dt_pool)
    
    # ✅ FIX: candidate_pool 120개만 watchlist에 전달 (재필터링 방지)
    pool_members = [m for m in members if str(m.get("code", "")).zfill(6) in {str(c).zfill(6) for c in pool_codes}]
    logger.info(
        "[PREP][CANDIDATE_POOL][DONE] selected=%s pool_members=%s",
        len(pool_codes),
        len(pool_members),
    )

    # Candidate pool source and pipeline stage counts for today's policy diagnostics.
    logger.info(
        "[WATCHLIST][STAGE_COUNTS] upstream_universe=%s raw_input=%s broader_scored=%s pool120=%s top50=%s final30=%s",
        len(members),
        len(pool_members),
        0,
        0,
        0,
        0,
    )

    t_watchlist = time.monotonic()
    logger.info("[PREP][HEARTBEAT] stage=watchlist_scoring status=start")

    def _watchlist_ohlcv(code: str, count: int = 120):
        df = _load_db_ohlcv_df(engine=engine, code=code, as_of=effective_as_of, count=count)
        return df, {"source": "db"}

    flow_provider = _make_flow_provider(engine)

    minervini_cfg = MinerviniConfig(rs_min_percentile=float(os.getenv("RS_MIN_PCTILE", "80")) / 100.0)
    
    # ✅ FIX: PREP에서는 절대 캐시를 사용하지 않음 (단일 진실 원천 확립)
    # ✅ FIX: pool_members (120개)만 전달하여 A단계 재필터링 방지
    watchlist_result = build_and_save_watchlist(
        engine=engine,
        env=env,
        strategy=os.getenv("PB1_WATCHLIST_STRATEGY", "pb1_watchlist"),
        as_of=effective_as_of,
        members=pool_members,
        ohlcv_provider=_watchlist_ohlcv,
        minervini_config={
            "rs_min_pctile": minervini_cfg.rs_min_percentile,
            "vcp_min_score": float(os.getenv("VCP_MIN_SCORE", "70")),
        },
        force_rebuild=True,
        use_cache=False,
        source_of_truth="candidate_pool",
        flow_provider=flow_provider,
        return_bundle=True,
    )
    if isinstance(watchlist_result, tuple):
        watchlist, watchlist_bundle = watchlist_result
    else:
        watchlist = watchlist_result
        watchlist_bundle = {
            "as_of": as_of,
            "weights": {
                "tech_weight": float(os.getenv("WATCHLIST_TECH_WEIGHT", "0.7")),
                "flow_weight": float(os.getenv("WATCHLIST_FLOW_WEIGHT", "0.3")),
            },
            "universe_scored": [],
            "pool120": [],
            "top50": [],
            "final30": watchlist,
            "reject_summary": {},
        }
    dt_watchlist = time.monotonic() - t_watchlist
    logger.info("[PREP][HEARTBEAT] stage=watchlist_scoring status=done")
    logger.info("[STAGE][DONE] name=%s dt=%.2f", "watchlist", dt_watchlist)

    # ✅ FIX: PREP에서는 현재 as_of만 사용, 이전 watchlist cache 재사용 금지
    shortage_reason = ""
    if len(watchlist) < finaln:
        shortage_reason = f"CURRENT_ASOF_WATCHLIST_TOO_SMALL:{len(watchlist)}<{finaln}"
        raise RuntimeError(shortage_reason)

    bundle_universe = watchlist_bundle.get("universe_scored", []) or []
    bundle_pool120 = watchlist_bundle.get("pool120", []) or []
    bundle_top50 = watchlist_bundle.get("top50", []) or []
    bundle_final30 = watchlist_bundle.get("final30", watchlist or []) or []
    stage_as_of["flow"] = effective_as_of.isoformat()
    stage_as_of["final30"] = effective_as_of.isoformat()

    logger.info(
        "[WATCHLIST][STAGE_COUNTS] upstream_universe=%s raw_input=%s broader_scored=%s pool120=%s top50=%s final30=%s",
        len(members),
        len(pool_members),
        len(bundle_universe),
        len(bundle_pool120),
        len(bundle_top50),
        len(bundle_final30),
    )

    # ✅ FIX: Use centralized contract validation function
    contract_mode = str(watchlist_bundle.get("contract_mode") or "candidate_pool_based")
    contract_failures = validate_watchlist_contract(
        universe_scored=bundle_universe,
        pool120=bundle_pool120,
        top50=bundle_top50,
        final30=bundle_final30,
        min_pool=pool_min,
        exact_final30=finaln,
        contract_mode=contract_mode,
        universe_scored_source="universe_filtered_from_raw120",
        candidate_pool_size=len(bundle_universe),
    )

    def _count_score_nonzero(rows: list[dict], keys: tuple[str, ...]) -> int:
        count = 0
        for row in rows:
            val = None
            for key in keys:
                if key in row and row.get(key) is not None:
                    val = row.get(key)
                    break
            try:
                if float(val or 0.0) > 0.0:
                    count += 1
            except (TypeError, ValueError):
                continue
        return count

    if contract_failures:
        shortage_reason = shortage_reason or ";".join(contract_failures)
        reject_summary = dict(watchlist_bundle.get("reject_summary", {}) or {})
        for reason in contract_failures:
            reject_summary[reason] = int(reject_summary.get(reason, 0)) + 1
        watchlist_bundle["reject_summary"] = reject_summary
        watchlist_bundle["shortage_reason"] = shortage_reason
        watchlist_bundle["degrade"] = {
            "enabled": True,
            "used": True,
            "reason": shortage_reason,
            "disabled_features": ["watchlist_pipeline_contract"],
        }
        reject_keys = set(reject_summary.keys())
        contract_source = "cache_bundle_missing" if "cache_bundle_stage_missing" in reject_keys else "watchlist_bundle"
        score_keys = ("score_final", "final_score", "score", "tech_score")
        logger.error(
            "[PREP][WATCHLIST][CONTRACT_FAIL] failures=%s allow_degrade=%s as_of=%s rows_universe=%s rows_pool120=%s rows_top50=%s rows_final30=%s score_nonzero={universe:%s,pool120:%s,top50:%s,final30:%s} source=%s",
            contract_failures,
            int(_env_true("PREP_CONTRACT_ALLOW_DEGRADE", "1") or _env_true("PB1_WATCHLIST_ALLOW_DEGRADE", "0") or allow_degraded_prep),
            as_of.isoformat(),
            len(bundle_universe),
            len(bundle_pool120),
            len(bundle_top50),
            len(bundle_final30),
            _count_score_nonzero(bundle_universe, score_keys),
            _count_score_nonzero(bundle_pool120, score_keys),
            _count_score_nonzero(bundle_top50, score_keys),
            _count_score_nonzero(bundle_final30, score_keys),
            contract_source,
        )
        
        # RECOVERY ROUTINE: Attempt to recover bundle from DB or rebuild
        recovery_attempted = False
        recovery_success = False
        
        if contract_source == "cache_bundle_missing" or any("contract_" in f for f in contract_failures):
            logger.warning(
                "[PREP][WATCHLIST][RECOVERY][START] attempting bundle recovery... source=%s",
                contract_source,
            )
            recovery_attempted = True
            
            # Step 1: Try DB recovery
            try:
                recovered_bundle = recover_bundle_from_db(
                    engine=engine,
                    env=env,
                    as_of=as_of,
                    min_pool=pool_min,
                    exact_top50=topk,
                    exact_final30=finaln,
                )
                
                if recovered_bundle and recovered_bundle.is_complete(
                    min_pool=pool_min,
                    exact_top50=topk,
                    exact_final30=finaln,
                ):
                    # DB recovery successful - use recovered bundle
                    bundle_universe = recovered_bundle.universe_scored
                    bundle_pool120 = recovered_bundle.pool120
                    bundle_top50 = recovered_bundle.top50
                    bundle_final30 = recovered_bundle.final30
                    watchlist = recovered_bundle.final30
                    
                    # Update watchlist_bundle
                    watchlist_bundle.update({
                        "universe_scored": bundle_universe,
                        "pool120": bundle_pool120,
                        "top50": bundle_top50,
                        "final30": bundle_final30,
                        "final_count": len(bundle_final30),
                        "shortage_reason": "",
                        "degrade": {
                            "enabled": False,
                            "used": False,
                            "reason": "recovered_from_db",
                            "disabled_features": [],
                        },
                    })
                    
                    recovery_success = True
                    logger.info(
                        "[PREP][WATCHLIST][RECOVERY][DB_SUCCESS] env=%s as_of=%s universe=%s pool120=%s top50=%s final30=%s",
                        env,
                        as_of,
                        len(bundle_universe),
                        len(bundle_pool120),
                        len(bundle_top50),
                        len(bundle_final30),
                    )
                else:
                    logger.warning(
                        "[PREP][WATCHLIST][RECOVERY][DB_FAIL] bundle incomplete or missing -> will try rebuild"
                    )
            except Exception as exc:
                logger.warning(
                    "[PREP][WATCHLIST][RECOVERY][DB_ERROR] err=%s -> will try rebuild",
                    exc,
                    exc_info=True,
                )
            
            # Step 2: If DB recovery failed, try rebuild
            if not recovery_success:
                try:
                    logger.warning(
                        "[PREP][WATCHLIST][RECOVERY][REBUILD][START] rebuilding bundle from scratch..."
                    )
                    
                    rebuilt_bundle = rebuild_bundle(
                        engine=engine,
                        env=env,
                        as_of=as_of,
                        members=members,
                        ohlcv_provider=_watchlist_ohlcv,
                        minervini_config={
                            "rs_min_pctile": minervini_cfg.rs_min_percentile,
                            "vcp_min_score": float(os.getenv("VCP_MIN_SCORE", "70")),
                        },
                        flow_provider=flow_provider,
                    )
                    
                    if rebuilt_bundle and rebuilt_bundle.is_complete(
                        min_pool=pool_min,
                        exact_top50=topk,
                        exact_final30=finaln,
                    ):
                        # Rebuild successful - save and use
                        save_bundle(
                            engine=engine,
                            env=env,
                            as_of=as_of,
                            bundle=rebuilt_bundle,
                        )
                        
                        bundle_universe = rebuilt_bundle.universe_scored
                        bundle_pool120 = rebuilt_bundle.pool120
                        bundle_top50 = rebuilt_bundle.top50
                        bundle_final30 = rebuilt_bundle.final30
                        watchlist = rebuilt_bundle.final30
                        
                        # Update watchlist_bundle
                        watchlist_bundle.update({
                            "universe_scored": bundle_universe,
                            "pool120": bundle_pool120,
                            "top50": bundle_top50,
                            "final30": bundle_final30,
                            "final_count": len(bundle_final30),
                            "shortage_reason": "",
                            "degrade": {
                                "enabled": False,
                                "used": False,
                                "reason": "recovered_by_rebuild",
                                "disabled_features": [],
                            },
                        })
                        
                        recovery_success = True
                        logger.info(
                            "[PREP][WATCHLIST][RECOVERY][REBUILD_SUCCESS] env=%s as_of=%s universe=%s pool120=%s top50=%s final30=%s",
                            env,
                            as_of,
                            len(bundle_universe),
                            len(bundle_pool120),
                            len(bundle_top50),
                            len(bundle_final30),
                        )
                    else:
                        logger.error(
                            "[PREP][WATCHLIST][RECOVERY][REBUILD_INCOMPLETE] bundle still incomplete after rebuild"
                        )
                except Exception as exc:
                    logger.error(
                        "[PREP][WATCHLIST][RECOVERY][REBUILD_ERROR] err=%s",
                        exc,
                        exc_info=True,
                    )
        
        # If recovery failed or not attempted, apply degradation policy
        if recovery_attempted and not recovery_success:
            logger.error(
                "[PREP][WATCHLIST][RECOVERY][FINAL_FAIL] all recovery attempts failed -> apply degrade policy"
            )
            watchlist_bundle["degrade"]["reason"] = f"recovery_failed:{shortage_reason}"
            watchlist_bundle["meta"] = watchlist_bundle.get("meta", {})
            watchlist_bundle["meta"]["recover_failed_reason"] = shortage_reason
        
        # Re-check contract failures after recovery
        contract_failures_after_recovery = []
        if len(bundle_universe) <= 0:
            contract_failures_after_recovery.append("contract_universe_scored_empty")
        if len(bundle_pool120) < pool_min:
            contract_failures_after_recovery.append(f"contract_pool120_too_small:{len(bundle_pool120)}<{pool_min}")
        if len(bundle_top50) < topk:
            contract_failures_after_recovery.append(f"contract_top50_too_small:{len(bundle_top50)}<{topk}")
        # ✅ FIX: Change from != to < for final30 validation
        if len(bundle_final30) < finaln:
            contract_failures_after_recovery.append(f"contract_final30_too_small:{len(bundle_final30)}<{finaln}")
        
        if contract_failures_after_recovery:
            logger.warning(
                "[PREP][WATCHLIST][CONTRACT_FAIL][AFTER_RECOVERY] failures=%s allow_degrade=%s",
                contract_failures_after_recovery,
                int(_env_true("PREP_CONTRACT_ALLOW_DEGRADE", "1") or _env_true("PB1_WATCHLIST_ALLOW_DEGRADE", "0") or allow_degraded_prep),
            )
        else:
            logger.info("[PREP][WATCHLIST][CONTRACT][RECOVERED] all contract requirements met after recovery")

        # Use post-recovery contract state for downstream payload/event decisions.
        contract_failures = contract_failures_after_recovery
        if not contract_failures:
            shortage_reason = ""
        
        allow_contract_degrade = _env_true("PREP_CONTRACT_ALLOW_DEGRADE", "1") or _env_true(
            "PB1_WATCHLIST_ALLOW_DEGRADE", "0"
        ) or allow_degraded_prep
        if contract_failures_after_recovery and not allow_contract_degrade:
            return 1

    watchlist_repo = WatchlistRepo(engine)
    watchlist_final_strategy = os.getenv("WATCHLIST_FINAL_STRATEGY_KEY", "pb1_watchlist_final").strip().lower()
    if watchlist:
        watchlist_repo.save_watchlist(
            env=env,
            strategy=watchlist_final_strategy,
            as_of=as_of,
            members=watchlist,
        )
        logger.info(
            "[PREP][WATCHLIST_FINAL][SAVE] strategy=%s as_of=%s n=%s",
            watchlist_final_strategy,
            as_of,
            len(watchlist),
        )
        stage_as_of["final30"] = as_of.isoformat()
        
        # final30 snapshot is a compatibility artifact only.
        from trader.final_list_store import save_final30
        final30_codes = [str(m.get("code") or "").zfill(6) for m in watchlist if m.get("code")]
        final30_meta = {
            "strategy": watchlist_final_strategy,
            "source": "prep_runner",
            "bundle_source": watchlist_bundle.get("degrade", {}).get("reason", "fresh_build"),
            "final_count": len(watchlist),
        }
        try:
            final30_path = save_final30(
                env=env,
                as_of=as_of.isoformat(),
                symbols=final30_codes,
                meta=final30_meta,
                overwrite=True,
            )
            logger.info(
                "[PREP][FINAL30_SNAPSHOT][SAVE] as_of=%s count=%s path=%s warn_only=1",
                as_of.isoformat(),
                len(final30_codes),
                final30_path,
            )
        except Exception as exc:
            logger.warning(
                "[PREP][FINAL30_SNAPSHOT][WARN] as_of=%s err=%s warn_only=1",
                as_of.isoformat(),
                exc,
            )

    run_id = os.getenv("TRADER_RUN_ID") or str(uuid4())
    os.environ["TRADER_RUN_ID"] = run_id
    ledger_repo = LedgerEventsRepo(engine)

    export_dir = RUNTIME_DIR / "watchlist" / as_of.strftime("%Y-%m-%d")
    final30_saved_df = pd.DataFrame(_safe_get(watchlist_bundle, "final30_saved", []))
    bundle_final30_scored_before_save = _safe_get(watchlist_bundle, "bundle_final30_scored_before_save")
    if bundle_final30_scored_before_save is None:
        bundle_final30_scored_before_save = _safe_get(watchlist_result, "bundle_final30_scored_before_save")
    if bundle_final30_scored_before_save is None:
        bundle_final30_scored_before_save = _safe_get(watchlist_bundle, "final30_scored")
    bundle_final30_scored_before_save_df = _as_dataframe(bundle_final30_scored_before_save)

    try:
        final30_scored_df_for_export, final30_source_label = _select_final30_scored_df_for_export(
            watchlist_result=watchlist_result,
            watchlist_bundle=watchlist_bundle,
            bundle_final30_scored_before_save_df=bundle_final30_scored_before_save_df,
            final30_saved_df=final30_saved_df,
        )
    except RuntimeError:
        raise RuntimeError("final30_scored_source_missing")

    scored_strategy = "pb1_watchlist_final_scored"
    scored_members = _build_scored_members(final30_scored_df_for_export)
    if scored_members:
        watchlist_repo.save_watchlist(
            env=env,
            strategy=scored_strategy,
            as_of=as_of,
            members=scored_members,
        )
        logger.info("[WATCHLIST][SAVE] strategy=%s members=%s", scored_strategy, len(scored_members))

        logger.info(
            "[WATCHLIST][SAVE_VERIFY][START] strategy=%s as_of=%s",
            scored_strategy,
            as_of.isoformat(),
        )
        loaded_scored_rows, loaded_scored_as_of = watchlist_repo.load_watchlist_scored(
            env=env,
            strategy=scored_strategy,
            as_of=as_of,
            allow_latest_fallback=False,
        )
        loaded_scored_df = pd.DataFrame(loaded_scored_rows or [])
        loaded_cols = [str(c) for c in loaded_scored_df.columns.tolist()]
        logger.info(
            "[WATCHLIST][SAVE_VERIFY][LOAD] rows=%s cols=%s used_as_of=%s",
            len(loaded_scored_df),
            loaded_cols,
            loaded_scored_as_of.isoformat() if loaded_scored_as_of else "",
        )

        roundtrip_missing = [col for col in CRITICAL_SCORED_COLS if col not in loaded_cols]
        if len(loaded_scored_df) != 30:
            roundtrip_missing = [*roundtrip_missing, f"rows_not_30:{len(loaded_scored_df)}"]

        if roundtrip_missing:
            logger.error("[WATCHLIST][SAVE_VERIFY][FAIL] missing_cols=%s", roundtrip_missing)
            logger.error("[PREP][FINAL30_SCORED][INTEGRITY_FAIL] db_roundtrip_missing_cols=%s", roundtrip_missing)
            raise RuntimeError(f"PREP_FINAL30_SCORED_INTEGRITY_FAIL:{roundtrip_missing}")

        logger.info(
            "[WATCHLIST][SAVE_VERIFY][OK] strategy=%s critical_scored_cols_present=1",
            scored_strategy,
        )
        scored_contract = watchlist_repo.verify_watchlist_scored_contract(
            env=env,
            as_of=as_of,
            strategy=scored_strategy,
            allow_latest_fallback=False,
        )
        exact_final_rows, _ = watchlist_repo.load_watchlist(
            env=env,
            strategy="pb1_watchlist_final",
            as_of=as_of,
            allow_latest_fallback=False,
        )
        scored_columns = set(scored_contract.get("columns") or [])
        critical_missing_fields = [col for col in CRITICAL_SCORED_COLS if col not in scored_columns]
        db_commit_ok = bool(
            len(exact_final_rows) == 30
            and int(scored_contract.get("rows") or 0) == 30
            and int(scored_contract.get("uniq_codes") or 0) == 30
            and int(scored_contract.get("null_critical") or 0) == 0
            and not critical_missing_fields
        )
        logger.info(
            "[PREP][DB_COMMIT][VERIFY] env=%s as_of=%s final=%s final_scored=%s required=30 ok=%s",
            env,
            as_of.isoformat(),
            len(exact_final_rows),
            scored_contract.get("rows"),
            int(db_commit_ok),
        )
        if scored_contract.get("rank_warn"):
            logger.warning(
                "[PREP][DB_COMMIT][WARN] rank_uniqueness_warn=1 source=%s",
                scored_contract.get("rank_source") or "unknown",
            )
        if not db_commit_ok:
            logger.error(
                "[PREP][COMMIT][FAIL] env=%s as_of=%s final=%s final_scored=%s uniq_codes=%s null_critical=%s missing_fields=%s critical_missing_fields=%s",
                env,
                as_of.isoformat(),
                len(exact_final_rows),
                scored_contract.get("rows"),
                scored_contract.get("uniq_codes"),
                scored_contract.get("null_critical"),
                scored_contract.get("missing_fields"),
                critical_missing_fields,
            )
            raise RuntimeError("PREP_DB_COMMIT_VERIFY_FAILED")

    logger.info("[FINAL30][FILE_MIRROR][TRY] targets=3")
    try:
        final30_file_results = _write_canonical_final30_scored_files(
            env=env,
            as_of=as_of.isoformat(),
            df=final30_scored_df_for_export,
        )
    except Exception as exc:
        logger.warning("[FINAL30][FILE_MIRROR][WARN] err=%s warn_only=1", exc)
        final30_file_results = {
            label: {"exists": False, "bytes": 0, "rows": 0, "json_ok": False}
            for label in build_final30_scored_paths(repo_root().resolve(), env, as_of.isoformat()).keys()
        }
    prep_repo_root = repo_root().resolve()
    prep_cwd = Path.cwd().resolve()
    final30_paths = build_final30_scored_paths(prep_repo_root, env, as_of.isoformat())
    logger.info(
        "[PREP][FINAL30][PATHS] repo_root=%s cwd=%s paths=%s",
        prep_repo_root,
        prep_cwd,
        {label: str(path) for label, path in final30_paths.items()},
    )
    for label, path in final30_paths.items():
        path_info = final30_file_results.get(label, {})
        logger.info(
            "[PREP][FINAL30][PATH] label=%s path=%s exists=%s bytes=%s rows=%s",
            label,
            str(path),
            int(bool(path_info.get("exists"))),
            int(path_info.get("bytes") or 0),
            int(path_info.get("rows") or 0),
        )
    logger.info(
        "[FINAL30][FILE_MIRROR][DONE] success=%s failed=%s warn_only=1",
        sum(1 for info in final30_file_results.values() if bool(info.get("exists")) and int(info.get("bytes") or 0) > 0 and int(info.get("rows") or 0) > 0),
        sum(1 for info in final30_file_results.values() if not bool(info.get("exists")) or int(info.get("bytes") or 0) <= 0 or int(info.get("rows") or 0) <= 0),
    )
    logger.info(
        "[PREP][SIGNALS][FINAL30_JSON] path=%s count=%s source=canonical_scored_contract",
        final30_paths["signals"],
        int(len(final30_scored_df_for_export)),
    )

    frames = {
        "universe_scored": pd.DataFrame(watchlist_bundle.get("universe_scored", [])),
        "pool120": pd.DataFrame(watchlist_bundle.get("pool120", [])),
        "top50": pd.DataFrame(watchlist_bundle.get("top50", [])),
        "final30": final30_scored_df_for_export,
    }

    # Single as_of contract across PREP stages.
    consistent = int(all(v == as_of.isoformat() for v in stage_as_of.values()))
    logger.info(
        "[PREP][ASOF_CONSISTENCY] universe=%s ohlcv=%s derived=%s candidate_pool=%s watchlist=%s flow=%s final30=%s consistent=%s",
        stage_as_of.get("universe", ""),
        stage_as_of.get("ohlcv", ""),
        stage_as_of.get("derived", ""),
        stage_as_of.get("candidate_pool", ""),
        stage_as_of.get("watchlist", ""),
        stage_as_of.get("flow", ""),
        stage_as_of.get("final30", ""),
        consistent,
    )
    if consistent != 1:
        raise RuntimeError("PREP_ASOF_CONSISTENCY_FAILED")

    final30_df = final30_scored_df_for_export
    inmem_stats: dict[str, int] = {
        "tech_nonzero": 0,
        "final_nonzero": 0,
        "score_final_nonzero": 0,
        "breakout_nonzero": 0,
        "pullback_nonzero": 0,
        "momentum_nonzero": 0,
    }
    if final30_df is not None and not final30_df.empty:
        logger.info(
            "[PREP][EXPORT][FINAL30][SOURCE] label=%s rows=%s",
            final30_source_label,
            int(len(final30_df)),
        )
        logger.info("[PREP][EXPORT][FINAL30][COLUMNS] cols=%s", sorted(final30_df.columns.tolist()))
        logger.info(
            "[PREP][EXPORT][FINAL30][FIELDS] has_tech_score=%s has_score_final=%s has_breakout_score=%s has_pullback_score=%s has_momentum_score=%s",
            int(resolve_score_column(final30_df, "tech") is not None),
            int(resolve_score_column(final30_df, "final") is not None),
            int(resolve_score_column(final30_df, "breakout") is not None),
            int(resolve_score_column(final30_df, "pullback") is not None),
            int(resolve_score_column(final30_df, "momentum") is not None),
        )
        final30_stats, inmem_cols = _collect_final30_nonzero_stats(final30_df)
        score_final_nonzero = int(final30_stats["score_final_nonzero"])
        final_score_nonzero = int(final30_stats["final_nonzero"])
        tech_score_nonzero = int(final30_stats["tech_nonzero"])
        breakout_nonzero = int(final30_stats["breakout_nonzero"])
        pullback_nonzero = int(final30_stats["pullback_nonzero"])
        momentum_nonzero = int(final30_stats["momentum_nonzero"])
        inmem_stats = {
            "tech_nonzero": tech_score_nonzero,
            "final_nonzero": final_score_nonzero,
            "score_final_nonzero": score_final_nonzero,
            "breakout_nonzero": breakout_nonzero,
            "pullback_nonzero": pullback_nonzero,
            "momentum_nonzero": momentum_nonzero,
        }
        logger.info(
            "[PREP][EXPORT][FINAL30][INMEM] rows=%s tech_nonzero=%s final_nonzero=%s score_final_nonzero=%s breakout_nonzero=%s pullback_nonzero=%s momentum_nonzero=%s",
            int(len(final30_df)),
            tech_score_nonzero,
            final_score_nonzero,
            score_final_nonzero,
            breakout_nonzero,
            pullback_nonzero,
            momentum_nonzero,
        )
    runtime_exported = False
    logger.info("[PREP][HEARTBEAT] stage=export status=start")
    try:
        export_watchlist_bundle(
            out_dir=export_dir,
            frames_dict=frames,
            meta_dict={
                "as_of": as_of.isoformat(),
                "env": env,
                "weights": watchlist_bundle.get("weights", {}),
                "weights_effective": watchlist_bundle.get("weights_effective", watchlist_bundle.get("weights", {})),
                "formula": watchlist_bundle.get("formula", ""),
                "reject_summary": watchlist_bundle.get("reject_summary", {}),
                "expected_finaln": finaln,
                "final_count": int(watchlist_bundle.get("final_count", len(watchlist or []))),
                "shortage_reason": shortage_reason,
                "degrade": watchlist_bundle.get("degrade", {}),
                "contract_failures": contract_failures,
            },
        )
        runtime_exported = True
        exporter_final30_df = pd.read_csv(export_dir / "final30.csv")
        post_stats_full, post_cols = _collect_final30_nonzero_stats(exporter_final30_df)
        post_stats = {
            "tech_nonzero": int(post_stats_full["tech_nonzero"]),
            "final_nonzero": int(post_stats_full["final_nonzero"]),
            "score_final_nonzero": int(post_stats_full["score_final_nonzero"]),
            "breakout_nonzero": int(post_stats_full["breakout_nonzero"]),
            "pullback_nonzero": int(post_stats_full["pullback_nonzero"]),
            "momentum_nonzero": int(post_stats_full["momentum_nonzero"]),
        }
        rhs_source_label = "exporter_final30_df"
        lhs_has_required = _has_required_final30_score_fields(final30_scored_df_for_export)
        rhs_has_required = _has_required_final30_score_fields(exporter_final30_df)
        logger.info(
            "[PREP][EXPORT][CONSISTENCY] lhs=%s rhs=%s",
            final30_source_label,
            rhs_source_label,
        )
        logger.info(
            "[PREP][EXPORT][CONSISTENCY][FIELDS] lhs_has_tech=%s rhs_has_tech=%s lhs_has_final=%s rhs_has_final=%s lhs_has_breakout=%s rhs_has_breakout=%s lhs_has_pullback=%s rhs_has_pullback=%s lhs_has_momentum=%s rhs_has_momentum=%s",
            "tech_score" in final30_scored_df_for_export.columns,
            "tech_score" in exporter_final30_df.columns,
            (("score_final" in final30_scored_df_for_export.columns) or ("final_score" in final30_scored_df_for_export.columns)),
            (("score_final" in exporter_final30_df.columns) or ("final_score" in exporter_final30_df.columns)),
            "breakout_score" in final30_scored_df_for_export.columns,
            "breakout_score" in exporter_final30_df.columns,
            "pullback_score" in final30_scored_df_for_export.columns,
            "pullback_score" in exporter_final30_df.columns,
            "momentum_score" in final30_scored_df_for_export.columns,
            "momentum_score" in exporter_final30_df.columns,
        )
        if not lhs_has_required or not rhs_has_required:
            logger.error(
                "[PREP][EXPORT][CONSISTENCY][MISSING_FIELDS] lhs_source=%s rhs_source=%s lhs_cols=%s rhs_cols=%s",
                final30_source_label,
                rhs_source_label,
                sorted(final30_scored_df_for_export.columns.tolist()),
                sorted(exporter_final30_df.columns.tolist()),
            )
            raise RuntimeError("final30_scored_fields_missing_for_consistency")

        _debug_compare_nonzero(
            key="final30_tech",
            inmem=inmem_stats["tech_nonzero"],
            exported=post_stats["tech_nonzero"],
            lhs_source=final30_source_label,
            rhs_source=rhs_source_label,
            lhs_col=inmem_cols.get("tech"),
            rhs_col=post_cols.get("tech"),
        )
        _debug_compare_nonzero(
            key="final30_score_final",
            inmem=inmem_stats["score_final_nonzero"],
            exported=post_stats["score_final_nonzero"],
            lhs_source=final30_source_label,
            rhs_source=rhs_source_label,
            lhs_col=inmem_cols.get("final"),
            rhs_col=post_cols.get("final"),
        )
        _debug_compare_nonzero(
            key="final30_breakout",
            inmem=inmem_stats["breakout_nonzero"],
            exported=post_stats["breakout_nonzero"],
            lhs_source=final30_source_label,
            rhs_source=rhs_source_label,
            lhs_col=inmem_cols.get("breakout"),
            rhs_col=post_cols.get("breakout"),
        )
        _debug_compare_nonzero(
            key="final30_pullback",
            inmem=inmem_stats["pullback_nonzero"],
            exported=post_stats["pullback_nonzero"],
            lhs_source=final30_source_label,
            rhs_source=rhs_source_label,
            lhs_col=inmem_cols.get("pullback"),
            rhs_col=post_cols.get("pullback"),
        )
        _debug_compare_nonzero(
            key="final30_momentum",
            inmem=inmem_stats["momentum_nonzero"],
            exported=post_stats["momentum_nonzero"],
            lhs_source=final30_source_label,
            rhs_source=rhs_source_label,
            lhs_col=inmem_cols.get("momentum"),
            rhs_col=post_cols.get("momentum"),
        )
        _assert_same_nonzero("final30_tech", inmem_stats["tech_nonzero"], post_stats["tech_nonzero"])
        _assert_same_nonzero("final30_score_final", inmem_stats["score_final_nonzero"], post_stats["score_final_nonzero"])
        _assert_same_nonzero("final30_breakout", inmem_stats["breakout_nonzero"], post_stats["breakout_nonzero"])
        _assert_same_nonzero("final30_pullback", inmem_stats["pullback_nonzero"], post_stats["pullback_nonzero"])
        _assert_same_nonzero("final30_momentum", inmem_stats["momentum_nonzero"], post_stats["momentum_nonzero"])
    except Exception:
        strict_export = _env_true("PREP_EXPORT_STRICT", "1")
        logger.exception(
            "[PREP][EXPORT][FAIL] as_of=%s out_dir=%s strict=%s",
            as_of,
            export_dir,
            int(strict_export),
        )
        _handle_export_consistency_failure(strict_export=strict_export)
    logger.info("[PREP][HEARTBEAT] stage=export status=done")

    # ✅ FIX: Initialize prep_status and flow_coverage early
    prep_status = "DONE"
    flow_coverage = 0.0
    degraded_exclude_flow = False
    final30_file_failures = [
        f"final30_file_contract_missing:{label}"
        for label, info in final30_file_results.items()
        if not bool(info.get("exists")) or int(info.get("bytes") or 0) <= 0 or int(info.get("rows") or 0) <= 0
    ]
    final30_file_contract_ok = len(final30_scored_df_for_export) > 0 and not final30_file_failures
    logger.info(
        "[PREP][FINAL30_FILE][CONTRACT] ok=%s warn_only=1 failures=%s",
        int(final30_file_contract_ok),
        final30_file_failures,
    )
    if final30_file_failures:
        logger.warning(
            "[PREP][FINAL30_FILE][WARN] repo_root=%s cwd=%s failures=%s",
            prep_repo_root,
            prep_cwd,
            final30_file_failures,
        )

    final_df = frames.get("final30", pd.DataFrame())
    if final_df is None or final_df.empty:
        logger.error("[PREP][DEGRADED] reason=empty_final30 as_of=%s", as_of)
        prep_status = "DEGRADED"
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            strategy="pb1_pullback_close",
            run_window="prep",
            event_type="PREP_DEGRADED",
            ts=now_kst(),
            ok=False,
            reasons=["empty_final30"],
            payload_json={"as_of": as_of.isoformat(), "final_count": 0, "flow_coverage": 0.0},
        )
        if allow_degraded_prep:
            logger.warning("[PREP][DEGRADED][ALLOW] reason=empty_final30 -> exit=0")
            return 0
        return 1

    # ✅ FIX: Flow validation - check DB-level coverage on final30 symbols
    final30_codes = [str(row.get("code", "")).zfill(6) for row in watchlist if row.get("code")]
    flow_coverage = 0.0
    
    if final30_codes:
        from trader.db.repos import DerivedFlowRepo
        flow_repo = DerivedFlowRepo(engine)
        
        try:
            # Load flow data from DB for final30 symbols
            flow_rows = flow_repo.load_for_as_of(env=env, as_of=as_of, symbols=final30_codes)
            covered = {str(r.get("symbol", "")).zfill(6) for r in flow_rows if not r.get("flow_missing", True)}
            flow_coverage = len(covered) / max(len(final30_codes), 1)
            flow_missing_count = len(final30_codes) - len(covered)
            
            logger.info(
                "[FLOW][DB][COVERAGE] covered=%s/%s coverage=%.1f%% as_of=%s",
                len(covered),
                len(final30_codes),
                flow_coverage * 100.0,
                as_of,
            )
        except Exception as exc:
            logger.warning(
                "[FLOW][DB][COVERAGE][FAIL] err=%s -> coverage=0%%",
                exc,
                exc_info=True,
            )
            flow_coverage = 0.0
            flow_missing_count = len(final30_codes)
    else:
        flow_missing_count = 0
    
    # Flow validation policy
    prep_status = "DONE"
    degraded_exclude_flow = False
    
    if flow_coverage < 0.5:
        # Flow coverage < 50% → FAIL
        logger.error(
            "[PREP][FLOW][FAIL] coverage=%.1f%% < 50%% as_of=%s allow_flow_degraded=%s",
            flow_coverage * 100.0,
            as_of,
            int(allow_flow_degraded_prep),
        )
        
        if not allow_flow_degraded_prep:
            raise RuntimeError(f"FLOW_COVERAGE_TOO_LOW:{flow_coverage:.2f}")
        else:
            prep_status = "DEGRADED"
            degraded_exclude_flow = True
            logger.warning("[PREP][FLOW][DEGRADED] tech-only mode enabled -> continue")
    
    elif flow_coverage < 0.8:
        # Flow coverage 50%-80% → DEGRADED
        logger.warning(
            "[PREP][FLOW][DEGRADED] coverage=%.1f%% between 50%%-80%% as_of=%s",
            flow_coverage * 100.0,
            as_of,
        )
        prep_status = "DEGRADED"
        degraded_exclude_flow = False  # Keep flow data but mark as degraded
    
    else:
        # Flow coverage >= 80% → OK
        logger.info(
            "[PREP][FLOW][OK] coverage=%.1f%% >= 80%% as_of=%s",
            flow_coverage * 100.0,
            as_of,
        )

    # Prepare metric columns for export
    core_metric_cols = ["rs_pctile", "vcp_score", "atr_pct", "trend_score", "pullback_pct"]
    flow_metric_cols = ["foreign_20_ratio", "inst_20_ratio", "flow_score"]
    
    metric_cols = [col for col in core_metric_cols if col in final_df.columns]
    if not degraded_exclude_flow:
        flow_available_cols = [col for col in flow_metric_cols if col in final_df.columns]
        metric_cols.extend(flow_available_cols)

    available_cols = metric_cols
    if available_cols:
        sample = final_df[available_cols].fillna(0.0)
        zero_ratio = float((sample == 0.0).all(axis=1).mean()) if len(sample) > 0 else 1.0
        if zero_ratio >= 0.8:
            logger.error(
                "[PREP][METRICS][MOSTLY_ZERO] ratio=%.3f as_of=%s metric_scope=%s",
                zero_ratio,
                as_of,
                "core_only" if degraded_exclude_flow else "core_plus_flow",
            )
            
            if not allow_degraded_prep:
                prep_status = "FAIL"
            else:
                prep_status = "DEGRADED"

    report_failmode_soft = _env_true("REPORT_FAILMODE_SOFT", "1")
    try:
        pdf_path = generate_watchlist_pdf(
            as_of=as_of,
            output_dir=export_dir,
            pool120=watchlist_bundle.get("pool120", []),
            top50=watchlist_bundle.get("top50", []),
            final30=watchlist_bundle.get("final30", watchlist or []),
            reject_summary=watchlist_bundle.get("reject_summary", {}),
            weights=watchlist_bundle.get("weights", {}),
            formula=watchlist_bundle.get("formula", ""),
        )
        logger.info("[PDF] wrote %s", pdf_path)
    except Exception:
        if report_failmode_soft:
            logger.exception("[REPORT][WATCHLIST][PDF][FAIL][SOFT] as_of=%s output_dir=%s", as_of, export_dir)
        else:
            logger.exception("[REPORT][WATCHLIST][PDF][FAIL][HARD] as_of=%s output_dir=%s", as_of, export_dir)
            raise
    
    payload = to_jsonable(
        {
            "as_of": as_of.isoformat(),
            "env": env,
            "symbols": len(symbols),
            "ohlcv_prefetch_mode": prefetch_mode,
            "delta": delta_result,
            "full": full_result,
            "derived_upserted": derived_upserted,
            "universe_size": len(members),
            "pool_size": len(pool_codes or []),
            "final_size": len(watchlist or []),
            "runtime_exported": runtime_exported,
            "flow_coverage": flow_coverage,
            "contract_failures": contract_failures,
            "contract_mode": contract_mode,
            "final30_file_contract_ok": final30_file_contract_ok,
            "final30_file_failures": final30_file_failures,
            "final_source": final30_source_label,
            "prep_status": prep_status,
            "durations_sec": {
                "ohlcv_delta": round(dt_ohlcv, 2),
                "derived": round(dt_derived, 2),
                "candidate_pool": round(dt_pool, 2),
                "watchlist": round(dt_watchlist, 2),
                "total": round(time.monotonic() - t0, 2),
            },
        }
    )
    
    # ✅ FIX: Mutually exclusive PREP event logging
    if prep_status == "FAIL":
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            strategy="pb1_pullback_close",
            run_window="prep",
            event_type="PREP_FAIL",
            ts=now_kst(),
            ok=False,
            reasons=contract_failures + ["flow_coverage_too_low" if flow_coverage < 0.5 else ""],
            payload_json=payload,
        )
        logger.error(
            "[LEDGER_EVENT] event_type=PREP_FAIL as_of=%s contract_failures=%s flow_coverage=%.1f%%",
            as_of.isoformat(),
            contract_failures,
            flow_coverage * 100.0,
        )
        return 1
    
    elif prep_status == "DEGRADED":
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            strategy="pb1_pullback_close",
            run_window="prep",
            event_type="PREP_DEGRADED",
            ts=now_kst(),
            ok=False,
            reasons=contract_failures + ["flow_coverage_degraded" if flow_coverage < 0.8 else ""],
            payload_json=payload,
        )
        logger.warning(
            "[LEDGER_EVENT] event_type=PREP_DEGRADED as_of=%s contract_failures=%s flow_coverage=%.1f%%",
            as_of.isoformat(),
            contract_failures,
            flow_coverage * 100.0,
        )
        if allow_degraded_prep:
            logger.warning("[PREP][DEGRADED][ALLOW] -> exit=0")
            return 0
        return 1
    
    else:
        # prep_status == "DONE"
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            strategy="pb1_pullback_close",
            run_window="prep",
            event_type="PREP_DONE",
            ts=now_kst(),
            payload_json=payload,
        )
        logger.info(
            "[LEDGER_EVENT] event_type=PREP_DONE as_of=%s symbols=%s flow_coverage=%.1f%%",
            as_of.isoformat(),
            len(symbols),
            flow_coverage * 100.0,
        )

    # Final diagnostic logging for bundle validation
    bundle_source = watchlist_bundle.get("degrade", {}).get("reason", "unknown")
    if watchlist_bundle.get("degrade", {}).get("used"):
        bundle_source = f"degraded:{bundle_source}"
    else:
        bundle_source = watchlist_bundle.get("meta", {}).get("source", "fresh_build")
    
    # Verify DB saved counts
    watchlist_repo = WatchlistRepo(engine)
    saved_counts = {}
    for strategy_key in ["pb1_universe_scored", "pb1_pool120", "pb1_top50", "pb1_watchlist_final"]:
        try:
            rows, _ = watchlist_repo.load_watchlist(
                env=env,
                strategy=strategy_key,
                as_of=as_of,
                allow_latest_fallback=False,
            )
            saved_counts[strategy_key] = len(rows) if rows else 0
        except Exception:
            saved_counts[strategy_key] = -1  # Error indicator
    
    logger.info(
        "[PREP][BUNDLE][FINAL_STATE] as_of=%s bundle_source=%s "
        "rows_universe=%s rows_pool120=%s rows_top50=%s rows_final30=%s "
        "saved_universe=%s saved_pool120=%s saved_top50=%s saved_final30=%s",
        as_of.isoformat(),
        bundle_source,
        len(bundle_universe),
        len(bundle_pool120),
        len(bundle_top50),
        len(bundle_final30),
        saved_counts.get("pb1_universe_scored", -1),
        saved_counts.get("pb1_pool120", -1),
        saved_counts.get("pb1_top50", -1),
        saved_counts.get("pb1_watchlist_final", -1),
    )
    
    # Alert if any saved counts are 0 (data loss indicator)
    missing_keys = [k for k, v in saved_counts.items() if v == 0]
    if missing_keys:
        logger.error(
            "[PREP][BUNDLE][DATA_LOSS_DETECTED] missing_strategies=%s as_of=%s -> CRITICAL: intermediate stages not saved to DB",
            missing_keys,
            as_of.isoformat(),
        )

    logger.info(
        "[PREP][DONE] as_of=%s source=%s universe=%s pool120=%s top50=%s final30=%s flow_coverage=%.1f final_source=%s contract_mode=%s dt=%.2f",
        as_of.isoformat(),
        bundle_source,
        len(bundle_universe),
        len(bundle_pool120),
        len(bundle_top50),
        len(bundle_final30),
        flow_coverage * 100.0,
        final30_source_label,
        contract_mode,
        time.monotonic() - t0,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
