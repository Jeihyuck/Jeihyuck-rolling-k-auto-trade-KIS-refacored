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
    FINAL30_SCORED_REQUIRED_ROWS,
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
    save_bundle_aux,
    WatchlistBundle,
    validate_watchlist_contract,
)
from trader.data.ohlcv_provider import (
    compute_required_prefetch_days,
    find_symbols_with_insufficient_history,
    upsert_ohlcv_delta,
)
from trader.exporter import export_watchlist_bundle
from trader.final30_quality import (
    build_canonical_prep_verdict,
    normalize_final30_contract_row,
    summarize_entry_style_distribution,
    summarize_final30_quality,
    verify_final30_scored_rows,
)
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
    resolve_trade_context,
    resolve_derived_as_of,
)
from trader.runtime_paths import build_final30_scored_paths, get_final30_artifact_paths, repo_root
from trader.path_contract import read_final30_file_rows, write_final30_mirrors, verify_final30_mirrors
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
    "close",
    "ma20",
    "ma50",
    "ma150",
    "pullback_pct",
    "entry_style_selected",
]
FINAL30_STRICT_REQUIRED_FIELDS = [
    "code",
    "close",
    "ma20",
    "ma50",
    "ma150",
    "atr_pct",
    "rs_percentile",
    "breakout_score",
    "pullback_score",
    "momentum_score",
    "tech_score",
    "score_final",
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
    providers = ["kis", "pykrx"]
    flow_cache: dict[tuple[str, str, int], tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]] = {}
    provider_state: dict[str, dict[str, Any]] = {
        "kis": {"enabled": True, "disabled_reason": "", "fail_count": 0, "success_count": 0, "disable_logged": False, "reason_counts": {}},
        "pykrx": {"enabled": True, "disabled_reason": "", "fail_count": 0, "success_count": 0, "disable_logged": False, "reason_counts": {}},
    }
    kis_api = None
    kis_init_failed = False

    def _empty_df() -> pd.DataFrame:
        return pd.DataFrame(columns=["date", "net_buy"])

    def _disable_provider(name: str, reason: str) -> None:
        state = provider_state.get(name) or {}
        if not state or not state.get("enabled", True):
            return
        state["enabled"] = False
        state["disabled_reason"] = str(reason or "unknown")
        if not state.get("disable_logged"):
            logger.warning("[FLOW][PROVIDER_DISABLE] provider=%s reason=%s", name, state["disabled_reason"])
            state["disable_logged"] = True

    def _record_provider_failure(name: str, reason: str) -> None:
        state = provider_state.get(name) or {}
        reasons = dict(state.get("reason_counts") or {})
        reason_key = str(reason or "unknown")
        reasons[reason_key] = int(reasons.get(reason_key, 0)) + 1
        state["reason_counts"] = reasons

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

    def _provider_pykrx(code: str, flow_as_of: date, _window: int) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        state = provider_state["pykrx"]
        if not state.get("enabled", True):
            return _empty_df(), _empty_df(), {
                "ok": False,
                "provider": "pykrx",
                "reason": state.get("disabled_reason") or "provider_disabled",
                "detail": "run_level_disabled",
            }
        try:
            from pykrx import stock
        except Exception as exc:
            state["fail_count"] += 1
            _record_provider_failure("pykrx", "import_failed")
            _disable_provider("pykrx", "import_failed")
            detail = str(exc).replace("\n", " ")[:200]
            logger.warning("[FLOW][PYKRX][FAIL] code=%s ymd=%s reason=import_failed detail=%s", code, flow_as_of.strftime("%Y%m%d"), detail)
            return _empty_df(), _empty_df(), {"ok": False, "provider": "pykrx", "reason": "import_failed", "detail": detail}

        ymd = flow_as_of.strftime("%Y%m%d")
        fail_reason = "unexpected_exception"
        fail_detail = ""
        try:
            fr_all = stock.get_market_net_purchases_of_equities_by_ticker(ymd, ymd, "ALL", "외국인")
            inst_all = stock.get_market_net_purchases_of_equities_by_ticker(ymd, ymd, "ALL", "기관합계")
            if fr_all is None or inst_all is None:
                raise ValueError("provider_return_none")
            if getattr(fr_all, "empty", True) or getattr(inst_all, "empty", True):
                raise ValueError("provider_return_empty")
            fr_row = _resolve_code_from_df(fr_all, code)
            inst_row = _resolve_code_from_df(inst_all, code)
            if fr_row.empty or inst_row.empty:
                raise ValueError("symbol_row_missing")
            fr_net = _extract_net_buy(fr_row)
            inst_net = _extract_net_buy(inst_row)
            if fr_net is None or inst_net is None:
                raise ValueError("numeric_parse_failed")
            foreign_df = pd.DataFrame([{"date": flow_as_of, "net_buy": fr_net}])
            inst_df = pd.DataFrame([{"date": flow_as_of, "net_buy": inst_net}])
            state["success_count"] += 1
            return foreign_df, inst_df, {"ok": True, "provider": "pykrx", "reason": "", "detail": ""}
        except Exception as exc:
            requests_json_err = None
            try:
                import requests  # type: ignore
                requests_json_err = requests.exceptions.JSONDecodeError
            except Exception:
                requests_json_err = None
            if isinstance(exc, json.JSONDecodeError):
                fail_reason = "JSONDecodeError"
            elif requests_json_err is not None and isinstance(exc, requests_json_err):
                fail_reason = "RequestsJSONDecodeError"
            elif isinstance(exc, ValueError):
                fail_reason = "ValueError"
            else:
                fail_reason = type(exc).__name__
            fail_detail = str(exc).replace("\n", " ")[:200]

        state["fail_count"] += 1
        _record_provider_failure("pykrx", fail_reason)
        logger.warning("[FLOW][PYKRX][FAIL] code=%s ymd=%s reason=%s detail=%s", code, ymd, fail_reason, fail_detail)
        if int(state.get("fail_count") or 0) >= 5:
            _disable_provider("pykrx", "repeated_parse_failures")
        return _empty_df(), _empty_df(), {"ok": False, "provider": "pykrx", "reason": fail_reason, "detail": fail_detail}

    def _provider_kis(code: str, flow_as_of: date, _window: int) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        nonlocal kis_api, kis_init_failed
        state = provider_state["kis"]
        if not state.get("enabled", True):
            return _empty_df(), _empty_df(), {
                "ok": False,
                "provider": "kis",
                "reason": state.get("disabled_reason") or "provider_disabled",
                "detail": "run_level_disabled",
            }
        if kis_init_failed:
            _disable_provider("kis", "init_failed")
            return _empty_df(), _empty_df(), {"ok": False, "provider": "kis", "reason": "init_failed", "detail": "init_failed"}
        if kis_api is None:
            try:
                from trader.kis_wrapper import KisAPI
                kis_api = KisAPI(kis_env=os.getenv("KIS_ENV", "practice"))
            except Exception as exc:
                kis_init_failed = True
                state["fail_count"] += 1
                _record_provider_failure("kis", "init_failed")
                _disable_provider("kis", "init_failed")
                detail = str(exc).replace("\n", " ")[:200]
                return _empty_df(), _empty_df(), {"ok": False, "provider": "kis", "reason": "init_failed", "detail": detail}

        def _response_reason(resp: dict[str, Any]) -> tuple[str, str]:
            msg = str(resp.get("error") or resp.get("msg1") or "").lower()
            if "breaker open" in msg or "fast_fail" in msg:
                return "breaker_open", msg[:200]
            if "invalid" in msg and "token" in msg:
                return "invalid_token", msg[:200]
            if "http" in msg and any(code in msg for code in ("500", "502", "503", "504")):
                return "http_5xx", msg[:200]
            return "unexpected_exception", msg[:200]

        attempts = 0
        refreshed = False
        retried_5xx = False
        while attempts < 3:
            attempts += 1
            try:
                resp = kis_api.inquire_investor(code, "KOSDAQ")
            except Exception as exc:
                reason = "breaker_open" if "breaker open" in str(exc).lower() else "unexpected_exception"
                if reason == "breaker_open":
                    logger.warning("[FLOW][KIS][DISABLE] reason=breaker_open")
                    _disable_provider("kis", reason)
                state["fail_count"] += 1
                _record_provider_failure("kis", reason)
                return _empty_df(), _empty_df(), {"ok": False, "provider": "kis", "reason": reason, "detail": str(exc)[:200]}

            if resp.get("ok"):
                inv = resp.get("inv") or {}
                foreign_raw = inv.get("frgn_ntby_qty", inv.get("frgn_ntby_tr_pbmn"))
                inst_raw = inv.get("orgn_ntby_qty", inv.get("orgn_ntby_tr_pbmn"))
                try:
                    foreign_net = float(foreign_raw) if foreign_raw is not None else None
                    inst_net = float(inst_raw) if inst_raw is not None else None
                except Exception:
                    foreign_net, inst_net = None, None
                if foreign_net is None or inst_net is None:
                    state["fail_count"] += 1
                    _record_provider_failure("kis", "numeric_parse_failed")
                    return _empty_df(), _empty_df(), {"ok": False, "provider": "kis", "reason": "numeric_parse_failed", "detail": "investor_fields_invalid"}
                state["success_count"] += 1
                return pd.DataFrame([{"date": flow_as_of, "net_buy": foreign_net}]), pd.DataFrame([{"date": flow_as_of, "net_buy": inst_net}]), {"ok": True, "provider": "kis", "reason": "", "detail": ""}

            reason, detail = _response_reason(resp)
            if reason == "breaker_open":
                logger.warning("[FLOW][KIS][DISABLE] reason=breaker_open")
                _disable_provider("kis", reason)
                state["fail_count"] += 1
                _record_provider_failure("kis", reason)
                return _empty_df(), _empty_df(), {"ok": False, "provider": "kis", "reason": reason, "detail": detail}
            if reason == "invalid_token" and not refreshed:
                refreshed = True
                try:
                    kis_api.refresh_token()
                except Exception:
                    pass
                continue
            if reason == "http_5xx" and not retried_5xx:
                retried_5xx = True
                continue
            if reason in {"invalid_token", "http_5xx"}:
                _disable_provider("kis", reason)
            state["fail_count"] += 1
            return _empty_df(), _empty_df(), {"ok": False, "provider": "kis", "reason": reason, "detail": detail}

        state["fail_count"] += 1
        _record_provider_failure("kis", "unexpected_exception")
        return _empty_df(), _empty_df(), {"ok": False, "provider": "kis", "reason": "unexpected_exception", "detail": "retry_exhausted"}

    provider_map = {
        "pykrx": _provider_pykrx,
        "kis": _provider_kis,
    }

    logger.info("[FLOW][SOURCE] providers=%s priority=kis->pykrx", providers)

    def _provider(code: str, as_of: date, window: int):
        flow_as_of = _resolve_flow_as_of(requested_as_of=as_of)
        cache_key = (str(code).zfill(6), flow_as_of.isoformat(), int(window))
        cached = flow_cache.get(cache_key)
        if cached is not None:
            return cached[0], cached[1], cached[2]

        attempted: list[str] = []
        reasons: list[str] = []
        for provider_name in providers:
            state = provider_state.get(provider_name) or {}
            if not state.get("enabled", True):
                reasons.append(f"{provider_name}:disabled:{state.get('disabled_reason') or 'provider_disabled'}")
                continue
            provider = provider_map.get(provider_name)
            if provider is None:
                continue
            attempted.append(provider_name)
            foreign_df, inst_df, meta = provider(str(code).zfill(6), flow_as_of, int(window))
            if foreign_df is not None and not foreign_df.empty and inst_df is not None and not inst_df.empty:
                meta["requested_as_of"] = as_of.isoformat()
                meta["flow_as_of"] = flow_as_of.isoformat()
                logger.info("[FLOW][PROVIDER_OK] code=%s as_of=%s provider=%s", str(code).zfill(6), flow_as_of, provider_name)
                flow_cache[cache_key] = (foreign_df, inst_df, meta)
                return foreign_df, inst_df, meta
            reasons.append(f"{provider_name}:{str((meta or {}).get('reason') or 'unknown')}")

        logger.warning(
            "[FLOW][WARN] all providers failed symbol=%s requested_as_of=%s flow_as_of=%s providers=%s reasons=%s",
            str(code).zfill(6),
            as_of,
            flow_as_of,
            attempted or providers,
            reasons,
        )
        fail_meta = {
            "ok": False,
            "provider": "none",
            "reason": ";".join(reasons) if reasons else "all_providers_failed",
            "detail": "all_providers_failed",
            "requested_as_of": as_of.isoformat(),
            "flow_as_of": flow_as_of.isoformat(),
        }
        flow_cache[cache_key] = (_empty_df(), _empty_df(), fail_meta)
        return _empty_df(), _empty_df(), fail_meta

    setattr(_provider, "provider_state", provider_state)
    return _provider

def _pick_as_of_date_always_prev() -> date:
    """PREP as_of 결정: AS_OF_OVERRIDE 우선, 없으면 전 거래일."""
    trade_ctx = resolve_trade_context(now=now_kst())
    return date.fromisoformat(str(trade_ctx["as_of"]))


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
    logger.info(
        "[DEBUG][FINAL30_SCORED][PRE_SAVE_NULLS] rows=%d close_null=%d ma20_null=%d ma50_null=%d ma150_null=%d atr_null=%d rs_null=%d score_null=%d score_final_null=%d",
        len(normalized_df),
        int(normalized_df["close"].isna().sum()) if "close" in normalized_df.columns else -1,
        int(normalized_df["ma20"].isna().sum()) if "ma20" in normalized_df.columns else -1,
        int(normalized_df["ma50"].isna().sum()) if "ma50" in normalized_df.columns else -1,
        int(normalized_df["ma150"].isna().sum()) if "ma150" in normalized_df.columns else -1,
        int(normalized_df["atr_pct"].isna().sum()) if "atr_pct" in normalized_df.columns else -1,
        int(normalized_df["rs_percentile"].isna().sum()) if "rs_percentile" in normalized_df.columns else -1,
        int(normalized_df["score"].isna().sum()) if "score" in normalized_df.columns else -1,
        int(normalized_df["score_final"].isna().sum()) if "score_final" in normalized_df.columns else -1,
    )
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
        payload = normalize_final30_contract_row(dict(row or {}))
        code = str(payload.get("code") or "").zfill(6)
        if not code:
            continue
        payload["code"] = code
        payload["rank"] = int(payload.get("rank") or payload.get("rank_final30") or idx)
        payload["as_of"] = payload.get("as_of")

        score_final = payload.get("score_final")
        if score_final is None:
            score_final = payload.get("final_score")
        if score_final is None:
            score_final = payload.get("score")
        canonical_score = float(score_final) if score_final is not None else None
        payload["score"] = canonical_score
        payload["score_final"] = canonical_score
        payload["final_score"] = canonical_score
        payload_meta = dict(payload)
        rows.append(
            {
                "code": code,
                "rank": int(payload.get("rank") or idx),
                **payload,
                "score": canonical_score,
                "meta": payload_meta,
            }
        )
    return rows


def _log_final30_field_trace(rows: list[dict[str, Any]], *, limit: int = 5) -> None:
    for row in (rows or [])[:limit]:
        normalized = normalize_final30_contract_row(row)
        logger.info(
            "[PREP][FINAL30][FIELD_TRACE] code=%s close=%s ma20=%s ma50=%s ma150=%s atr_pct=%s",
            normalized.get("code", ""),
            normalized.get("close"),
            normalized.get("ma20"),
            normalized.get("ma50"),
            normalized.get("ma150"),
            normalized.get("atr_pct"),
        )


def _log_entry_style_distribution(rows: list[dict[str, Any]] | pd.DataFrame, *, prefix: str) -> None:
    summary = summarize_entry_style_distribution(rows)
    logger.info(
        "[%s][FINAL30][ENTRY_STYLE_DISTRIBUTION] total=%s counts=%s",
        prefix,
        int(summary.get("total") or 0),
        dict(summary.get("counts") or {}),
    )


def decide_prep_trade_gate(hard_fail_reasons: list[str] | None, soft_fail_reasons: list[str] | None) -> dict[str, int | str]:
    hard_fail_reasons = list(hard_fail_reasons or [])
    soft_fail_reasons = list(soft_fail_reasons or [])
    has_hard = bool(hard_fail_reasons)
    has_soft = bool(soft_fail_reasons)
    status = "FAIL" if has_hard else ("WARN" if has_soft else "OK")
    quality_ok = 0 if has_hard else 1
    trade_can_proceed = 0 if has_hard else 1
    return {
        "status": status,
        "quality_ok": quality_ok,
        "soft_fail": int(has_soft),
        "trade_can_proceed": trade_can_proceed,
    }


def _strict_validate_final30_rows(rows: list[dict[str, Any]], *, source: str) -> dict[str, Any]:
    _log_entry_style_distribution(rows, prefix="PREP")
    result = verify_final30_scored_rows(
        rows,
        required_rows=FINAL30_SCORED_REQUIRED_ROWS,
        required_fields=FINAL30_STRICT_REQUIRED_FIELDS,
        source=source,
    )
    logger.info(
        "[PREP][FINAL30][STRICT_VALIDATE][%s] ok=%s rows=%s invalid_rows=%s errors=%s warnings=%s",
        source,
        int(bool(result.get("ok"))),
        int(result.get("rows") or 0),
        int(result.get("invalid_row_count") or 0),
        list(result.get("errors") or []),
        list(result.get("warnings") or []),
    )
    return result


def _strict_validate_final30_files(*, env: str, as_of: str) -> dict[str, Any]:
    path_results: dict[str, Any] = {}
    aggregate_errors: list[str] = []
    aggregate_ok = True
    for label, path in build_final30_scored_paths(repo_root(), env, as_of).items():
        rows, json_ok = read_final30_file_rows(path)
        validate = _strict_validate_final30_rows(rows, source=f"FILES:{label}") if json_ok and rows else {
            "ok": False,
            "rows": len(rows),
            "required_cols_ok": False,
            "errors": ["file_missing_or_unreadable"] if not path.exists() or not json_ok else ["rows_not_exact"],
        }
        info = {
            "path": path,
            "exists": bool(path.exists()),
            "json_ok": bool(json_ok),
            "rows_ok": int(validate.get("rows") or 0) == FINAL30_SCORED_REQUIRED_ROWS,
            "required_cols_ok": bool(validate.get("required_cols_ok")),
            "strict_quality_ok": bool(validate.get("ok")),
            "errors": list(validate.get("errors") or []),
            "rows": int(validate.get("rows") or 0),
        }
        logger.info("[PREP][FINAL30][STRICT_VALIDATE][FILES] label=%s result=%s", label, info)
        path_results[label] = info
        aggregate_ok = aggregate_ok and bool(info["strict_quality_ok"])
        aggregate_errors.extend(info["errors"])

    return {
        "ok": aggregate_ok,
        "errors": list(dict.fromkeys(aggregate_errors)),
        "paths": path_results,
    }


def _write_canonical_final30_scored_files(*, env: str, as_of: str, df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    payload_df = (df.copy() if df is not None else pd.DataFrame())
    for col in FINAL30_SCORED_EXPORT_COLS:
        if col not in payload_df.columns:
            payload_df[col] = None
    payload_df = payload_df[FINAL30_SCORED_EXPORT_COLS]
    payload_df["code"] = payload_df["code"].astype(str).str.zfill(6)
    payload = payload_df.to_dict(orient="records")
    critical_cols = [col for col in CRITICAL_SCORED_COLS if col in payload_df.columns]
    logger.info("[PREP][FINAL30][REPAIR][START] as_of=%s env=%s rows=%s", as_of, env, len(payload))
    file_results = write_final30_mirrors(
        repo_root=repo_root(),
        env=env,
        as_of=as_of,
        rows=payload,
        source="prep_final30_scored",
    )
    verify_results = verify_final30_mirrors(
        repo_root=repo_root(),
        env=env,
        as_of=as_of,
        expected_rows=len(payload) or None,
    )
    success_count = 0
    failed_count = 0
    for label, path in build_final30_scored_paths(repo_root(), env, as_of).items():
        info = dict(file_results.get(label, {}))
        verify_info = dict(verify_results.get(label, {}))
        info.update(verify_info)
        rows = int(info.get("rows") or 0)
        exists = int(bool(info.get("exists")))
        bytes_written = int(info.get("bytes") or 0)
        json_ok = int(bool(info.get("json_ok")))
        detected_critical_cols = sorted([col for col in critical_cols if payload and col in payload[0]])
        info["critical_cols"] = detected_critical_cols
        info["ok"] = bool(
            exists == 1
            and bytes_written > 0
            and json_ok == 1
            and len(payload) == FINAL30_SCORED_REQUIRED_ROWS
            and rows == FINAL30_SCORED_REQUIRED_ROWS
            and set(CRITICAL_SCORED_COLS).issubset(set(detected_critical_cols))
        )
        file_results[label] = info
        success_count += int(bool(info.get("ok")))
        failed_count += int(not bool(info.get("ok")))
        logger.info(
            "[PREP][FINAL30_SCORED][FILE_VERIFY] source=%s path=%s exists=%s bytes=%s json_ok=%s rows=%s critical_cols=%s",
            label,
            path,
            exists,
            bytes_written,
            json_ok,
            rows,
            detected_critical_cols,
        )
        if not info["ok"]:
            logger.warning(
                "[PREP][FINAL30_SCORED][VERIFY_FAIL] source=%s path=%s expected_rows=%s required_cols=%s",
                label,
                path,
                len(payload),
                critical_cols,
            )

    logger.info("[PREP][FINAL30][REPAIR][DONE] success=%s failed=%s", success_count, failed_count)

    logger.info(
        "[PREP][FINAL30_SCORED][FIELDS] has_score_final=%s has_tech_score=%s has_breakout_score=%s has_pullback_score=%s has_momentum_score=%s",
        int("score_final" in payload_df.columns),
        int("tech_score" in payload_df.columns),
        int("breakout_score" in payload_df.columns),
        int("pullback_score" in payload_df.columns),
        int("momentum_score" in payload_df.columns),
    )
    return file_results


def sync_prep_final30_file_mirror(*, env: str, as_of: str, df: pd.DataFrame) -> dict[str, Any]:
    logger.info("[PREP][FINAL30][FILE_MIRROR][SYNC_START] as_of=%s rows=%s", as_of, int(len(df) if df is not None else 0))
    file_results = _write_canonical_final30_scored_files(env=env, as_of=as_of, df=df)
    logger.info(
        "[PREP][FINAL30][FILE_MIRROR][SYNC_DONE] runtime=%s ledger=%s signals=%s",
        int(bool((file_results.get("runtime") or {}).get("ok"))),
        int(bool((file_results.get("ledger") or {}).get("ok"))),
        int(bool((file_results.get("signals") or {}).get("ok"))),
    )
    validation = _strict_validate_final30_files(env=env, as_of=as_of)
    validation_paths = dict(validation.get("paths") or {})
    logger.info(
        "[PREP][FINAL30][FILE_MIRROR][POST_VALIDATE] ok=%s runtime=%s ledger=%s signals=%s",
        int(bool(validation.get("ok"))),
        int((validation_paths.get("runtime") or {}).get("rows") or 0),
        int((validation_paths.get("ledger") or {}).get("rows") or 0),
        int((validation_paths.get("signals") or {}).get("rows") or 0),
    )
    return {
        "file_results": file_results,
        "validation": validation,
    }


def save_final30_scored_core(
    df_final30_scored: pd.DataFrame,
    as_of: date,
    env: str,
    *,
    engine: Any,
    final_strategy: str = "pb1_watchlist_final",
    watchlist_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    logger.info(
        "[FINAL30_SCORED][CORE_SAVE][START] as_of=%s env=%s rows=%s",
        as_of.isoformat(),
        env,
        int(len(df_final30_scored) if df_final30_scored is not None else 0),
    )
    final30_df = _as_dataframe(df_final30_scored)
    if final30_df.empty:
        raise RuntimeError("FINAL30_SCORED_CORE_SAVE_EMPTY")

    inmem_validation = _strict_validate_final30_rows(final30_df.to_dict(orient="records"), source="CORE_SAVE_INPUT")
    if not bool(inmem_validation.get("ok")):
        raise RuntimeError(f"FINAL30_SCORED_CORE_SAVE_CONTRACT_FAIL:{list(inmem_validation.get('errors') or [])}")

    scored_members = _build_scored_members(final30_df)
    uniq_codes = len({str((row or {}).get("code") or "").zfill(6) for row in scored_members if (row or {}).get("code")})
    if len(scored_members) != 30 or uniq_codes != 30:
        raise RuntimeError(f"FINAL30_SCORED_CORE_SAVE_ROWS_FAIL:rows={len(scored_members)} uniq_codes={uniq_codes}")

    watchlist_repo = WatchlistRepo(engine)
    base_members = list(watchlist_rows or scored_members)
    watchlist_repo.save_watchlist(
        env=env,
        strategy=final_strategy,
        as_of=as_of,
        members=base_members,
    )
    watchlist_repo.save_watchlist(
        env=env,
        strategy="pb1_watchlist_final_scored",
        as_of=as_of,
        members=scored_members,
    )
    exact_final_rows, _ = watchlist_repo.load_watchlist(
        env=env,
        strategy=final_strategy,
        as_of=as_of,
        allow_latest_fallback=False,
    )
    scored_contract = watchlist_repo.verify_watchlist_scored_contract(
        env=env,
        as_of=as_of,
        strategy="pb1_watchlist_final_scored",
        allow_latest_fallback=False,
    )
    scored_columns = set(scored_contract.get("columns") or [])
    critical_missing_fields = [col for col in CRITICAL_SCORED_COLS if col not in scored_columns]
    if not bool(scored_contract.get("ok")) or len(exact_final_rows) != 30 or critical_missing_fields:
        raise RuntimeError(
            "FINAL30_SCORED_CORE_SAVE_DB_FAIL:"
            f"rows={scored_contract.get('rows')} uniq_codes={scored_contract.get('uniq_codes')} missing={critical_missing_fields}"
        )
    logger.info("[FINAL30_SCORED][CORE_SAVE][DB_OK] rows=%s", int(scored_contract.get("rows") or 0))

    mirror_sync = sync_prep_final30_file_mirror(
        env=env,
        as_of=as_of.isoformat(),
        df=final30_df,
    )
    file_results = dict(mirror_sync.get("file_results") or {})
    validation = dict(mirror_sync.get("validation") or {})
    if not bool(validation.get("ok")):
        raise RuntimeError(f"FINAL30_SCORED_CORE_SAVE_FILE_FAIL:{list(validation.get('errors') or [])}")

    final30_paths = build_final30_scored_paths(repo_root(), env, as_of.isoformat())
    logger.info(
        "[FINAL30_SCORED][CORE_SAVE][RUNTIME_OK] rows=%s path=%s",
        int((file_results.get("runtime") or {}).get("rows") or 0),
        final30_paths["runtime"],
    )
    logger.info(
        "[FINAL30_SCORED][CORE_SAVE][LEDGER_OK] rows=%s path=%s",
        int((file_results.get("ledger") or {}).get("rows") or 0),
        final30_paths["ledger"],
    )

    reload_validation = _strict_validate_final30_files(env=env, as_of=as_of.isoformat())
    reload_contract = watchlist_repo.verify_watchlist_scored_contract(
        env=env,
        as_of=as_of,
        strategy="pb1_watchlist_final_scored",
        allow_latest_fallback=False,
    )
    if not bool(reload_validation.get("ok")) or not bool(reload_contract.get("ok")):
        raise RuntimeError(
            "FINAL30_SCORED_CORE_SAVE_RELOAD_FAIL:"
            f"db_ok={int(bool(reload_contract.get('ok')))} file_ok={int(bool(reload_validation.get('ok')))}"
        )
    logger.info(
        "[FINAL30_SCORED][CORE_SAVE][RELOAD_OK] db_rows=%s runtime_rows=%s ledger_rows=%s",
        int(reload_contract.get("rows") or 0),
        int((file_results.get("runtime") or {}).get("rows") or 0),
        int((file_results.get("ledger") or {}).get("rows") or 0),
    )
    logger.info("[FINAL30_SCORED][CORE_SAVE][DONE] rows=%s", len(scored_members))
    return {
        "db_rows": int(reload_contract.get("rows") or 0),
        "runtime_rows": int((file_results.get("runtime") or {}).get("rows") or 0),
        "ledger_rows": int((file_results.get("ledger") or {}).get("rows") or 0),
        "signals_rows": int((file_results.get("signals") or {}).get("rows") or 0),
        "runtime_path": final30_paths["runtime"],
        "ledger_path": final30_paths["ledger"],
        "signals_path": final30_paths["signals"],
        "scored_contract": reload_contract,
        "file_results": file_results,
        "validation": reload_validation,
        "exact_final_rows": exact_final_rows,
    }


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
    _ = bundle_final30_scored_before_save_df
    _ = final30_saved_df

    final30_scored_for_save = None
    final30_scored_source = ""

    if _safe_get(watchlist_result, "final30_scored") is not None:
        final30_scored_for_save = _as_dataframe(_safe_get(watchlist_result, "final30_scored")).copy(deep=True)
        final30_scored_source = "watchlist_result.final30_scored"
    elif _safe_get(watchlist_bundle, "final30_scored") is not None:
        final30_scored_for_save = _as_dataframe(_safe_get(watchlist_bundle, "final30_scored")).copy(deep=True)
        final30_scored_source = "watchlist_bundle.final30_scored"
    else:
        raise RuntimeError("FINAL30_SCORED_SOURCE_MISSING")

    logger.info(
        "[PREP][FINAL30_SCORED][SOURCE_SELECTED] source=%s rows=%d cols=%s",
        final30_scored_source,
        len(final30_scored_for_save),
        list(final30_scored_for_save.columns),
    )

    if "ma20" not in final30_scored_for_save.columns:
        raise RuntimeError("FINAL30_SCORED_SOURCE_MISSING_MA20_COLUMN")

    ma20_nulls = int(final30_scored_for_save["ma20"].isna().sum())
    logger.info(
        "[PREP][FINAL30_SCORED][SOURCE_CHECK] source=%s rows=%d ma20_null=%d",
        final30_scored_source,
        len(final30_scored_for_save),
        ma20_nulls,
    )

    if ma20_nulls > 0:
        bad_codes = final30_scored_for_save.loc[
            final30_scored_for_save["ma20"].isna(), "code"
        ].head(10).tolist()
        raise RuntimeError(
            f"INVALID_FINAL30_SCORED_SOURCE_BEFORE_SAVE ma20_nulls={ma20_nulls} sample_codes={bad_codes}"
        )

    return final30_scored_for_save, final30_scored_source


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
        save_intermediate_bundle=False,
    )
    watchlist_result_payload: Any = None
    if isinstance(watchlist_result, tuple):
        watchlist, watchlist_bundle = watchlist_result
        watchlist_result_payload = watchlist_bundle
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
        watchlist_result_payload = watchlist_result if isinstance(watchlist_result, dict) else watchlist_bundle
    dt_watchlist = time.monotonic() - t_watchlist
    logger.info("[PREP][HEARTBEAT] stage=watchlist_scoring status=done")
    logger.info("[STAGE][DONE] name=%s dt=%.2f", "watchlist", dt_watchlist)

    result_final30_scored_obj = _safe_get(watchlist_result_payload, "final30_scored")
    if result_final30_scored_obj is not None:
        result_final30_scored_df = _as_dataframe(result_final30_scored_obj)
        logger.info(
            "[DEBUG][OBJ][RESULT_FINAL30_SCORED] id=%s rows=%d ma20_null=%d",
            id(result_final30_scored_obj),
            len(result_final30_scored_df),
            int(result_final30_scored_df["ma20"].isna().sum()) if "ma20" in result_final30_scored_df.columns else -1,
        )

    bundle_final30_scored_obj = _safe_get(watchlist_bundle, "final30_scored")
    if bundle_final30_scored_obj is not None:
        bundle_final30_scored_df = _as_dataframe(bundle_final30_scored_obj)
        logger.info(
            "[DEBUG][OBJ][BUNDLE_FINAL30_SCORED] id=%s rows=%d ma20_null=%d",
            id(bundle_final30_scored_obj),
            len(bundle_final30_scored_df),
            int(bundle_final30_scored_df["ma20"].isna().sum()) if "ma20" in bundle_final30_scored_df.columns else -1,
        )

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
                        # Rebuild successful - defer broader bundle persistence until DONE_CORE.
                        logger.info("[BUNDLE][AUX][DEFER] reason=rebuild_bundle_ready_before_core as_of=%s", as_of)

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
        bundle_final30_scored_before_save = _safe_get(watchlist_result_payload, "bundle_final30_scored_before_save")
    if bundle_final30_scored_before_save is None:
        bundle_final30_scored_before_save = _safe_get(watchlist_bundle, "final30_scored")
    bundle_final30_scored_before_save_df = _as_dataframe(bundle_final30_scored_before_save)

    final30_scored_df_for_export, final30_source_label = _select_final30_scored_df_for_export(
        watchlist_result=watchlist_result_payload,
        watchlist_bundle=watchlist_bundle,
        bundle_final30_scored_before_save_df=bundle_final30_scored_before_save_df,
        final30_saved_df=final30_saved_df,
    )
    if isinstance(final30_scored_df_for_export, pd.DataFrame) and not final30_scored_df_for_export.empty:
        _flow_total = int(len(final30_scored_df_for_export))
        _flow_success = int(
            pd.to_numeric(final30_scored_df_for_export.get("flow_data_available", 0), errors="coerce")
            .fillna(0)
            .astype(int)
            .sum()
        ) if "flow_data_available" in final30_scored_df_for_export.columns else 0
        _flow_failed = max(_flow_total - _flow_success, 0)
        _flow_ratio = (_flow_failed / float(_flow_total)) if _flow_total > 0 else 0.0
        _flow_usage = (
            final30_scored_df_for_export["flow_provider_used"].fillna("none").astype(str).str.lower().value_counts().to_dict()
            if "flow_provider_used" in final30_scored_df_for_export.columns else {}
        )
        _flow_reasons = (
            final30_scored_df_for_export["flow_fail_reason"].fillna("").astype(str).str.strip().value_counts().to_dict()
            if "flow_fail_reason" in final30_scored_df_for_export.columns else {}
        )
        _flow_status = "ok"
        if _flow_success <= 0:
            _flow_status = "missing"
        elif _flow_failed > 0:
            _flow_status = "partial"
        if int(_flow_usage.get("none", 0)) > 0 and (int(_flow_usage.get("kis", 0)) == 0 or int(_flow_usage.get("pykrx", 0)) == 0):
            _flow_status = "degraded"
        final30_scored_df_for_export["flow_status"] = _flow_status
        final30_scored_df_for_export["flow_failed_ratio"] = _flow_ratio
        final30_scored_df_for_export["flow_success_symbols"] = _flow_success
        final30_scored_df_for_export["flow_failed_symbols"] = _flow_failed
        final30_scored_df_for_export["flow_provider_usage_kis"] = int(_flow_usage.get("kis", 0))
        final30_scored_df_for_export["flow_provider_usage_pykrx"] = int(_flow_usage.get("pykrx", 0))
        final30_scored_df_for_export["flow_provider_usage_none"] = int(_flow_usage.get("none", 0))
        final30_scored_df_for_export["flow_failure_reasons"] = json.dumps({k: int(v) for k, v in _flow_reasons.items() if k}, ensure_ascii=False)

    logger.info("[PREP][DONE_CORE][START] as_of=%s", as_of.isoformat())
    core_save_result = save_final30_scored_core(
        final30_scored_df_for_export,
        as_of,
        env,
        engine=engine,
        final_strategy=watchlist_final_strategy,
        watchlist_rows=watchlist,
    )
    exact_final_rows = list(core_save_result.get("exact_final_rows") or [])
    scored_contract = dict(core_save_result.get("scored_contract") or {})
    final30_file_results = dict(core_save_result.get("file_results") or {})
    final30_files_validation = dict(core_save_result.get("validation") or {})
    logger.info("[PREP][DONE_CORE][DB_OK] rows=%s", core_save_result.get("db_rows", 0))
    logger.info(
        "[PREP][DONE_CORE][RUNTIME_OK] rows=%s path=%s",
        core_save_result.get("runtime_rows", 0),
        core_save_result.get("runtime_path", ""),
    )
    logger.info(
        "[PREP][DONE_CORE][LEDGER_OK] rows=%s path=%s",
        core_save_result.get("ledger_rows", 0),
        core_save_result.get("ledger_path", ""),
    )
    core_marker_payload = {
        "as_of": as_of.isoformat(),
        "env": env,
        "final_count": int(core_save_result.get("db_rows") or 0),
        "flow_coverage": 0.0,
        "done_phase": "core",
        "runtime_rows": int(core_save_result.get("runtime_rows") or 0),
        "ledger_rows": int(core_save_result.get("ledger_rows") or 0),
        "signals_rows": int(core_save_result.get("signals_rows") or 0),
    }
    ledger_repo.append_event(
        env=env,
        run_id=run_id,
        strategy="pb1_pullback_close",
        run_window="prep",
        event_type="PREP_DONE",
        ts=now_kst(),
        payload_json=core_marker_payload,
    )
    logger.info("[PREP][DONE_CORE][MARKER_OK] as_of=%s", as_of.isoformat())
    logger.info("[PREP][DONE_CORE][DONE] as_of=%s rows=%s", as_of.isoformat(), core_save_result.get("db_rows", 0))
    logger.info("[PREP][DONE] as_of=%s status=DONE_CORE rows=%s", as_of.isoformat(), core_save_result.get("db_rows", 0))

    logger.info("[WATCHLIST][REUSE][FINAL30_SCORED] hit=True")
    logger.info("[WATCHLIST][REBUILD][SKIP] reason=existing_final30_scored")
    aux_bundle = WatchlistBundle(
        as_of=as_of.isoformat(),
        env=env,
        strategy=watchlist_final_strategy,
        universe_scored=list(bundle_universe),
        pool120=list(bundle_pool120),
        top50=list(bundle_top50),
        final30=final30_scored_df_for_export.to_dict(orient="records"),
        meta={
            "weights": watchlist_bundle.get("weights", {}),
            "weights_effective": watchlist_bundle.get("weights_effective", watchlist_bundle.get("weights", {})),
            "formula": watchlist_bundle.get("formula", ""),
            "reject_summary": watchlist_bundle.get("reject_summary", {}),
            "degrade": watchlist_bundle.get("degrade", {}),
            "source": "prep_runner_existing_final30_scored",
        },
    )
    aux_bundle_result = save_bundle_aux(
        engine=engine,
        env=env,
        as_of=as_of,
        bundle=aux_bundle,
    )
    core_done = True
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
    final30_quality: dict[str, Any] = summarize_final30_quality(pd.DataFrame())
    final30_quality_ok = False
    final30_quality_soft_fail = False
    final30_trade_can_proceed = False
    final30_gate_decision = decide_prep_trade_gate(["final30_missing"], [])
    if final30_df is not None and not final30_df.empty:
        _log_final30_field_trace(final30_df.to_dict(orient="records"))
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
        final30_quality = _strict_validate_final30_rows(final30_df.to_dict(orient="records"), source="INMEM")
        final30_gate_decision = decide_prep_trade_gate(
            list(final30_quality.get("hard_fail_reasons") or []),
            list(final30_quality.get("soft_fail_reasons") or []),
        )
        final30_quality_ok = bool(final30_gate_decision["quality_ok"])
        final30_quality_soft_fail = bool(final30_gate_decision["soft_fail"])
        final30_trade_can_proceed = bool(final30_gate_decision["trade_can_proceed"])
        logger.info(
            "[PREP][FINAL30][QUALITY] ok=%s soft_fail=%s rows=%s uniq_codes=%s momentum_monoculture=%s score_monoculture=%s",
            final30_quality_ok,
            final30_quality_soft_fail,
            final30_quality.get("rows"),
            final30_quality.get("uniq_codes"),
            final30_quality.get("momentum_monoculture"),
            final30_quality.get("score_monoculture"),
        )
        logger.info(
            "[PREP][QUALITY_GATE] hard_fail=%s soft_fail=%s status=%s trade_can_proceed=%s hard_fail_reasons=%s soft_fail_reasons=%s",
            int(bool(final30_quality.get("hard_fail_reasons") or [])),
            int(bool(final30_quality.get("soft_fail_reasons") or [])),
            final30_gate_decision["status"],
            int(final30_trade_can_proceed),
            list(final30_quality.get("hard_fail_reasons") or []),
            list(final30_quality.get("soft_fail_reasons") or []),
        )
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
    prep_dir = RUNTIME_DIR / "prep" / as_of.strftime("%Y-%m-%d")
    prep_dir.mkdir(parents=True, exist_ok=True)
    final30_locked_rows = final30_scored_df_for_export.to_dict(orient="records") if isinstance(final30_scored_df_for_export, pd.DataFrame) else []
    final30_locked_count = len(final30_locked_rows)
    fallbacks_used: list[str] = []
    degrade_info = dict(watchlist_bundle.get("degrade", {}) or {})
    degrade_reason = str(degrade_info.get("reason") or "").strip()
    if bool(degrade_info.get("used")) and degrade_reason:
        fallbacks_used.append(degrade_reason)
    if shortage_reason:
        fallbacks_used.append(str(shortage_reason))
    for failure in contract_failures or []:
        failure_s = str(failure).strip()
        if failure_s:
            fallbacks_used.append(failure_s)
    step_payloads = [
        {"name": "resolve_as_of", "in": 1, "out": 1, "source": "resolve_trade_context", "fallback": 0, "reason": "", "ok": 1, "can_proceed": 1},
        {"name": "build_universe", "in": len(bundle_universe), "out": len(bundle_universe), "source": "universe_scored", "fallback": int(bool(degrade_info.get("used"))), "reason": degrade_reason or "", "ok": int(len(bundle_universe) > 0), "can_proceed": int(len(bundle_universe) > 0)},
        {"name": "build_candidate_pool", "in": len(bundle_universe), "out": len(bundle_pool120), "source": "pool120", "fallback": int(bool(degrade_info.get("used"))), "reason": shortage_reason or "", "ok": int(len(bundle_pool120) >= min(pool_min, max(1, len(bundle_pool120)))), "can_proceed": int(len(bundle_pool120) > 0)},
        {"name": "build_watchlist", "in": len(bundle_pool120), "out": len(watchlist or []), "source": watchlist_final_strategy, "fallback": int(bool(degrade_info.get("used"))), "reason": shortage_reason or "", "ok": int(len(watchlist or []) >= min(finaln, max(1, len(watchlist or [])))), "can_proceed": int(len(watchlist or []) > 0)},
        {"name": "build_final30_locked", "in": len(watchlist or []), "out": final30_locked_count, "source": final30_source_label, "fallback": int(bool(fallbacks_used)), "reason": ",".join(fallbacks_used), "ok": int(final30_locked_count >= min(finaln, max(1, final30_locked_count))), "can_proceed": int(final30_locked_count > 0)},
    ]
    for step_payload in step_payloads:
        logger.info(
            "[PREP][STEP] name=%s in=%s out=%s source=%s fallback=%s ok=%s reason=%s can_proceed=%s",
            step_payload["name"],
            step_payload["in"],
            step_payload["out"],
            step_payload["source"],
            step_payload["fallback"],
            step_payload["ok"],
            step_payload["reason"],
            step_payload["can_proceed"],
        )
    prep_manifest = {
        "trade_date": now_kst().date().isoformat(),
        "as_of": as_of.isoformat(),
        "universe_count": len(bundle_universe),
        "candidate_pool_count": len(bundle_pool120),
        "watchlist_count": len(watchlist or []),
        "final30_count": final30_locked_count,
        "fallbacks_used": sorted(set(fallbacks_used)),
        "build_status": "DEGRADED" if fallbacks_used else str(final30_gate_decision["status"]),
        "final30_quality": final30_quality,
        "final30_quality_ok": final30_quality_ok,
        "final30_quality_soft_fail": final30_quality_soft_fail,
        "trade_can_proceed": int(final30_trade_can_proceed),
        "steps": step_payloads,
    }
    prep_manifest_path = prep_dir / "prep_manifest.json"
    prep_manifest_path.write_text(json.dumps(to_jsonable(prep_manifest), ensure_ascii=False, indent=2), encoding="utf-8")
    (prep_dir / "candidate_pool_snapshot.json").write_text(json.dumps(to_jsonable(bundle_pool120), ensure_ascii=False, indent=2), encoding="utf-8")
    (prep_dir / "watchlist_snapshot.json").write_text(json.dumps(to_jsonable(watchlist or []), ensure_ascii=False, indent=2), encoding="utf-8")
    (prep_dir / "final30_locked.json").write_text(json.dumps(to_jsonable(final30_locked_rows), ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[PREP][MANIFEST][SAVE] path=%s status=%s fallbacks=%s", prep_manifest_path, prep_manifest["build_status"], prep_manifest["fallbacks_used"])
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
        strict_export = False if core_done else _env_true("PREP_EXPORT_STRICT", "1")
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
    final30_file_failures.extend(list(final30_files_validation.get("errors") or []))
    final30_file_failures = list(dict.fromkeys(final30_file_failures))
    final30_file_contract_ok = len(final30_scored_df_for_export) > 0 and not final30_file_failures and bool(final30_files_validation.get("ok"))
    logger.info(
        "[PREP][FINAL30_SCORED][FILE_VERIFY] ok=%s failures=%s runtime=%s ledger=%s signals=%s",
        int(final30_file_contract_ok),
        final30_file_failures,
        int((final30_file_results.get("runtime") or {}).get("rows") or 0),
        int((final30_file_results.get("ledger") or {}).get("rows") or 0),
        int((final30_file_results.get("signals") or {}).get("rows") or 0),
    )
    if final30_file_failures:
        logger.error(
            "[PREP][FINAL30_FILE][FAIL] repo_root=%s cwd=%s failures=%s",
            prep_repo_root,
            prep_cwd,
            final30_file_failures,
        )
    strict_contract_failures = list(contract_failures or []) + list(final30_file_failures or []) + list(scored_contract.get("errors") or []) + list(final30_quality.get("errors") or [])
    if ((not final30_quality_ok) or (not bool(scored_contract.get("ok"))) or (not bool(final30_files_validation.get("ok")))) and not core_done:
        raise RuntimeError(
            "PREP_FINAL30_STRICT_VALIDATE_FAILED:"
            + ",".join(
                list(dict.fromkeys(
                    list(final30_quality.get("errors") or [])
                    + list(scored_contract.get("errors") or [])
                    + list(final30_files_validation.get("errors") or [])
                ))
            )
        )
    if bool(scored_contract) and final30_file_contract_ok:
        logger.info(
            "[PREP][FINAL30_SCORED][CONTRACT_OK] as_of=%s final=%s final_scored=%s runtime=%s ledger=%s signals=%s",
            as_of.isoformat(),
            len(exact_final_rows),
            int(scored_contract.get("rows") or 0),
            int((final30_file_results.get("runtime") or {}).get("rows") or 0),
            int((final30_file_results.get("ledger") or {}).get("rows") or 0),
            int((final30_file_results.get("signals") or {}).get("rows") or 0),
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
    
    flow_total_symbols = len(final30_codes)
    flow_success_symbols = 0
    flow_failed_symbols = 0
    flow_failed_ratio = 0.0
    flow_provider_usage_kis = 0
    flow_provider_usage_pykrx = 0
    flow_provider_usage_none = 0
    if isinstance(final_df, pd.DataFrame) and not final_df.empty:
        if "flow_data_available" in final_df.columns:
            flow_success_symbols = int(pd.to_numeric(final_df["flow_data_available"], errors="coerce").fillna(0).astype(int).sum())
            flow_failed_symbols = max(flow_total_symbols - flow_success_symbols, 0)
        if "flow_provider_used" in final_df.columns:
            usage = final_df["flow_provider_used"].fillna("none").astype(str).str.lower().value_counts().to_dict()
            flow_provider_usage_kis = int(usage.get("kis", 0))
            flow_provider_usage_pykrx = int(usage.get("pykrx", 0))
            flow_provider_usage_none = int(usage.get("none", 0))
        else:
            flow_provider_usage_none = flow_total_symbols
    else:
        flow_failed_symbols = flow_total_symbols
        flow_provider_usage_none = flow_total_symbols
    if flow_total_symbols > 0:
        flow_failed_ratio = flow_failed_symbols / float(flow_total_symbols)
    flow_fail_reason_counts: dict[str, int] = {}
    if isinstance(final_df, pd.DataFrame) and ("flow_fail_reason" in final_df.columns):
        reason_series = final_df["flow_fail_reason"].fillna("").astype(str).str.strip()
        for reason, count in reason_series.value_counts().to_dict().items():
            if reason:
                flow_fail_reason_counts[str(reason)] = int(count)
    provider_state = dict(getattr(flow_provider, "provider_state", {}) or {})
    for provider_name in ("kis", "pykrx"):
        reason_counts = dict(((provider_state.get(provider_name) or {}).get("reason_counts")) or {})
        for reason, count in reason_counts.items():
            key = f"{provider_name}:{reason}"
            flow_fail_reason_counts[key] = int(flow_fail_reason_counts.get(key, 0)) + int(count or 0)
    kis_disabled_reason = str(((provider_state.get("kis") or {}).get("disabled_reason")) or "")
    pykrx_disabled_reason = str(((provider_state.get("pykrx") or {}).get("disabled_reason")) or "")
    logger.info("[FLOW][OPTIONAL_MODE] enabled=1 blocking=0")
    logger.info(
        "[FLOW][SUMMARY] total=%s success=%s failed=%s failed_ratio=%.3f kis=%s pykrx=%s none=%s reasons=%s kis_disabled_reason=%s pykrx_disabled_reason=%s",
        flow_total_symbols,
        flow_success_symbols,
        flow_failed_symbols,
        flow_failed_ratio,
        flow_provider_usage_kis,
        flow_provider_usage_pykrx,
        flow_provider_usage_none,
        flow_fail_reason_counts,
        kis_disabled_reason or "-",
        pykrx_disabled_reason or "-",
    )

    # Flow validation policy
    prep_status = "DONE"
    degraded_exclude_flow = False
    
    if flow_coverage < 0.5:
        logger.warning(
            "[PREP][FLOW][WARN] coverage=%.1f%% < 50%% as_of=%s allow_flow_degraded=%s core_done=%s",
            flow_coverage * 100.0,
            as_of,
            int(allow_flow_degraded_prep),
            int(core_done),
        )
        prep_status = "WARN" if core_done else "DEGRADED"
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

    prep_status = str(final30_gate_decision["status"])
    if strict_contract_failures:
        prep_status = "FAIL"
        final30_quality.setdefault("hard_fail_reasons", [])
        if "final30_contract_fail" not in final30_quality["hard_fail_reasons"]:
            final30_quality["hard_fail_reasons"].append("final30_contract_fail")
        final30_quality_ok = False
        final30_trade_can_proceed = False
        final30_gate_decision["status"] = "FAIL"
        final30_gate_decision["quality_ok"] = 0
        final30_gate_decision["trade_can_proceed"] = 0
        logger.error(
            "[PREP][FINAL30_SCORED][CONTRACT_FAIL] as_of=%s failures=%s",
            as_of.isoformat(),
            strict_contract_failures,
        )
    canonical_verdict = build_canonical_prep_verdict(
        quality=final30_quality,
        flow_failed_ratio=flow_failed_ratio,
        flow_fail_reason_counts=flow_fail_reason_counts,
    )
    final30_quality = dict(canonical_verdict["quality"])
    final30_gate_decision = {
        "status": canonical_verdict["status"],
        "quality_ok": canonical_verdict["quality_ok"],
        "soft_fail": canonical_verdict["soft_fail"],
        "trade_can_proceed": canonical_verdict["trade_can_proceed"],
    }
    final30_quality_ok = bool(canonical_verdict["quality_ok"])
    final30_quality_soft_fail = bool(canonical_verdict["soft_fail"])
    final30_trade_can_proceed = bool(canonical_verdict["trade_can_proceed"])
    flow_provider_total_failure = int(flow_failed_ratio >= 1.0)
    logger.info(
        "[PREP][FLOW_OPTIONAL] failed_ratio=%.3f provider_total_failure=%s blocking=0 trade_can_proceed_unchanged=%s",
        flow_failed_ratio,
        flow_provider_total_failure,
        int(final30_gate_decision["trade_can_proceed"]),
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
            final30_quality.setdefault("soft_fail_reasons", [])
            if "metrics_mostly_zero" not in final30_quality["soft_fail_reasons"]:
                final30_quality["soft_fail_reasons"].append("metrics_mostly_zero")
            canonical_verdict = build_canonical_prep_verdict(
                quality=final30_quality,
                flow_failed_ratio=flow_failed_ratio,
                flow_fail_reason_counts=flow_fail_reason_counts,
            )
            final30_quality = dict(canonical_verdict["quality"])
            final30_gate_decision["status"] = canonical_verdict["status"]
            final30_gate_decision["quality_ok"] = canonical_verdict["quality_ok"]
            final30_gate_decision["soft_fail"] = canonical_verdict["soft_fail"]
            final30_gate_decision["trade_can_proceed"] = canonical_verdict["trade_can_proceed"]
            final30_quality_ok = bool(canonical_verdict["quality_ok"])
            final30_quality_soft_fail = bool(canonical_verdict["soft_fail"])
            final30_trade_can_proceed = bool(canonical_verdict["trade_can_proceed"])
            prep_status = str(canonical_verdict["status"])
    flow_status = "ok"
    if flow_total_symbols <= 0:
        flow_status = "missing"
    elif flow_success_symbols <= 0:
        flow_status = "missing"
    elif flow_provider_usage_none > 0 and (flow_provider_usage_kis == 0 or flow_provider_usage_pykrx == 0):
        flow_status = "degraded"
    elif flow_failed_symbols > 0:
        flow_status = "partial"

    logger.info(
        "[PREP][FINAL_QUALITY] status=%s hard_fail_count=%s soft_fail_count=%s hard_fail_reasons=%s soft_fail_reasons=%s flow_optional=1 trade_can_proceed=%s",
        final30_gate_decision["status"],
        len(list(final30_quality.get("hard_fail_reasons") or [])),
        len(list(final30_quality.get("soft_fail_reasons") or [])),
        list(final30_quality.get("hard_fail_reasons") or []),
        list(final30_quality.get("soft_fail_reasons") or []),
        int(final30_gate_decision["trade_can_proceed"]),
    )

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
            "flow_total_symbols": flow_total_symbols,
            "flow_success_symbols": flow_success_symbols,
            "flow_failed_symbols": flow_failed_symbols,
            "flow_failed_ratio": flow_failed_ratio,
            "flow_provider_usage_kis": flow_provider_usage_kis,
            "flow_provider_usage_pykrx": flow_provider_usage_pykrx,
            "flow_provider_usage_none": flow_provider_usage_none,
            "flow_fail_reason_counts": flow_fail_reason_counts,
            "flow_failure_reasons": flow_fail_reason_counts,
            "flow_status": flow_status,
            "flow_optional": True,
            "flow_blocking_enabled": False,
            "kis_disabled_reason": kis_disabled_reason,
            "pykrx_disabled_reason": pykrx_disabled_reason,
            "contract_failures": strict_contract_failures,
            "contract_mode": contract_mode,
            "final30_file_contract_ok": final30_file_contract_ok,
            "final30_file_failures": final30_file_failures,
            "final_source": final30_source_label,
            "prep_status": prep_status,
            "final30_quality": final30_quality,
            "final30_quality_ok": final30_quality_ok,
            "final30_quality_soft_fail": final30_quality_soft_fail,
            "trade_can_proceed": int(final30_gate_decision["trade_can_proceed"]),
            "canonical_quality": {
                "status": final30_gate_decision["status"],
                "quality_ok": int(final30_gate_decision["quality_ok"]),
                "soft_fail": int(final30_gate_decision["soft_fail"]),
                "trade_can_proceed": int(final30_gate_decision["trade_can_proceed"]),
                "hard_fail_reasons": list(final30_quality.get("hard_fail_reasons") or []),
                "soft_fail_reasons": list(final30_quality.get("soft_fail_reasons") or []),
                "flow_optional": True,
            },
            "durations_sec": {
                "ohlcv_delta": round(dt_ohlcv, 2),
                "derived": round(dt_derived, 2),
                "candidate_pool": round(dt_pool, 2),
                "watchlist": round(dt_watchlist, 2),
                "total": round(time.monotonic() - t0, 2),
            },
        }
    )
    prep_manifest["build_status"] = prep_status
    prep_manifest["final30_quality"] = final30_quality
    prep_manifest["final30_quality_ok"] = final30_quality_ok
    prep_manifest["final30_quality_soft_fail"] = final30_quality_soft_fail
    prep_manifest["trade_can_proceed"] = int(final30_trade_can_proceed)
    prep_manifest["flow_total_symbols"] = flow_total_symbols
    prep_manifest["flow_success_symbols"] = flow_success_symbols
    prep_manifest["flow_failed_symbols"] = flow_failed_symbols
    prep_manifest["flow_failed_ratio"] = flow_failed_ratio
    prep_manifest["flow_provider_usage_kis"] = flow_provider_usage_kis
    prep_manifest["flow_provider_usage_pykrx"] = flow_provider_usage_pykrx
    prep_manifest["flow_provider_usage_none"] = flow_provider_usage_none
    prep_manifest["flow_fail_reason_counts"] = flow_fail_reason_counts
    prep_manifest["flow_failure_reasons"] = flow_fail_reason_counts
    prep_manifest["flow_status"] = flow_status
    prep_manifest["flow_optional"] = True
    prep_manifest["flow_blocking_enabled"] = False
    prep_manifest["kis_disabled_reason"] = kis_disabled_reason
    prep_manifest["pykrx_disabled_reason"] = pykrx_disabled_reason
    prep_manifest["canonical_quality"] = {
        "status": final30_gate_decision["status"],
        "quality_ok": int(final30_gate_decision["quality_ok"]),
        "soft_fail": int(final30_gate_decision["soft_fail"]),
        "trade_can_proceed": int(final30_gate_decision["trade_can_proceed"]),
        "hard_fail_reasons": list(final30_quality.get("hard_fail_reasons") or []),
        "soft_fail_reasons": list(final30_quality.get("soft_fail_reasons") or []),
        "flow_optional": True,
    }
    prep_manifest_path.write_text(json.dumps(to_jsonable(prep_manifest), ensure_ascii=False, indent=2), encoding="utf-8")
    if prep_status != "FAIL":
        mirror_sync = sync_prep_final30_file_mirror(
            env=env,
            as_of=as_of.isoformat(),
            df=final30_scored_df_for_export,
        )
        final30_file_results = dict(mirror_sync.get("file_results") or {})
        final30_files_validation = dict(mirror_sync.get("validation") or {})
        final30_file_failures = list(dict.fromkeys(list(final30_file_failures) + list(final30_files_validation.get("errors") or [])))
        final30_file_contract_ok = bool(final30_files_validation.get("ok"))
        if not final30_file_contract_ok:
            strict_contract_failures = list(dict.fromkeys(list(strict_contract_failures) + list(final30_file_failures)))
            final30_quality.setdefault("hard_fail_reasons", [])
            if "final30_file_contract_fail" not in final30_quality["hard_fail_reasons"]:
                final30_quality["hard_fail_reasons"].append("final30_file_contract_fail")
            final30_gate_decision["status"] = "FAIL"
            final30_gate_decision["quality_ok"] = 0
            final30_gate_decision["trade_can_proceed"] = 0
            final30_quality_ok = False
            final30_trade_can_proceed = False
            prep_status = "FAIL"
            if core_done:
                logger.warning("[PREP][AUX][WARN] final30 mirror resync failed after DONE_CORE failures=%s", final30_file_failures)
            else:
                prep_status = "FAIL"
    payload["prep_status"] = prep_status
    payload["final30_file_contract_ok"] = final30_file_contract_ok
    payload["final30_file_failures"] = list(final30_file_failures)
    payload["contract_failures"] = list(strict_contract_failures)
    prep_manifest["build_status"] = prep_status
    prep_manifest["trade_can_proceed"] = int(final30_gate_decision["trade_can_proceed"])
    prep_manifest_path.write_text(json.dumps(to_jsonable(prep_manifest), ensure_ascii=False, indent=2), encoding="utf-8")
    
    # ✅ FIX: Mutually exclusive PREP event logging
    if core_done:
        if prep_status == "FAIL":
            logger.warning("[PREP][AUX][WARN] status=FAIL after DONE_CORE failures=%s", strict_contract_failures)
        elif prep_status == "DEGRADED":
            logger.warning("[PREP][AUX][WARN] status=DEGRADED after DONE_CORE")
        elif prep_status == "WARN":
            logger.warning("[PREP][AUX][WARN] status=WARN after DONE_CORE")
    elif prep_status == "FAIL":
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            strategy="pb1_pullback_close",
            run_window="prep",
            event_type="PREP_FAIL",
            ts=now_kst(),
            ok=False,
            reasons=strict_contract_failures + (["flow_coverage_too_low"] if flow_coverage < 0.5 else []),
            payload_json=payload,
        )
        logger.error(
            "[LEDGER_EVENT] event_type=PREP_FAIL as_of=%s contract_failures=%s flow_coverage=%.1f%%",
            as_of.isoformat(),
            strict_contract_failures,
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
            reasons=(strict_contract_failures or []) + (["flow_coverage_degraded"] if flow_coverage < 0.8 else []),
            payload_json=payload,
        )
        logger.warning(
            "[LEDGER_EVENT] event_type=PREP_DEGRADED as_of=%s contract_failures=%s flow_coverage=%.1f%%",
            as_of.isoformat(),
            strict_contract_failures,
            flow_coverage * 100.0,
        )
        if allow_degraded_prep:
            logger.warning("[PREP][DEGRADED][ALLOW] -> exit=0")
            return 0
        return 1

    elif prep_status == "WARN":
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            strategy="pb1_pullback_close",
            run_window="prep",
            event_type="PREP_DONE",
            ts=now_kst(),
            payload_json=payload,
        )
        logger.warning(
            "[LEDGER_EVENT] event_type=PREP_DONE as_of=%s status=WARN quality_ok=%s soft_fail=%s trade_can_proceed=%s",
            as_of.isoformat(),
            int(final30_gate_decision["quality_ok"]),
            int(final30_gate_decision["soft_fail"]),
            int(final30_gate_decision["trade_can_proceed"]),
        )
    
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
    for strategy_key in ["pb1_universe_scored", "pb1_pool120", "pb1_top50", "pb1_watchlist_final_scored"]:
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
        saved_counts.get("pb1_watchlist_final_scored", -1),
    )
    
    # Alert if any intermediate stages not saved (separate from final30)
    intermediate_missing = [k for k, v in saved_counts.items() if v == 0 and k != "pb1_watchlist_final_scored"]
    if intermediate_missing:
        logger.warning(
            "[PREP][INTERMEDIATE_STAGE_SAVE][FAIL] missing_strategies=%s as_of=%s",
            intermediate_missing,
            as_of.isoformat(),
        )
    
    # Check final30 saved (this is what matters for trade)
    final30_saved = saved_counts.get("pb1_watchlist_final_scored", 0)
    if final30_saved == 0:
        logger.error(
            "[PREP][FINAL30_CONTRACT][FAIL] not_saved as_of=%s",
            as_of.isoformat(),
        )
    else:
        logger.info(
            "[PREP][FINAL30_CONTRACT][OK] rows=%d as_of=%s",
            final30_saved,
            as_of.isoformat(),
        )

    logger.info(
        "[PREP][DONE] as_of=%s source=%s universe=%s pool120=%s top50=%s final30=%s flow_coverage=%.1f final_source=%s contract_mode=%s status=%s quality_ok=%s soft_fail=%s trade_can_proceed=%s aux_ok=%s dt=%.2f",
        as_of.isoformat(),
        bundle_source,
        len(bundle_universe),
        len(bundle_pool120),
        len(bundle_top50),
        len(bundle_final30),
        flow_coverage * 100.0,
        final30_source_label,
        contract_mode,
        prep_status,
        int(final30_gate_decision["quality_ok"]),
        int(final30_gate_decision["soft_fail"]),
        int(final30_gate_decision["trade_can_proceed"]),
        int(bool(aux_bundle_result.get("ok", False))) if isinstance(aux_bundle_result, dict) else 0,
        time.monotonic() - t0,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
