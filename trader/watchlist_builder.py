"""PB1 Watchlist Builder - 120 -> 50 -> 30 unified pipeline."""
from __future__ import annotations

import logging
import os
import time
from collections import Counter
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import Engine

from trader.config import RS_BENCHMARK, RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, RS_MIN_PCTILE
from trader.db.repos import DerivedFlowRepo, DerivedMinerviniRepo, WatchlistRepo
from trader.flow_score import calculate_final_score, calculate_flow_score
from trader.time_coerce import to_date

logger = logging.getLogger(__name__)

FlowProvider = Callable[[str, date, int], Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]]


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_bool(key: str, default: bool) -> bool:
    val = os.getenv(key, str(default)).lower()
    return val in ("1", "true", "yes", "on")


def _normalize_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    normalized = df.copy()
    normalized.columns = [str(col).strip().lower().replace(" ", "_") for col in normalized.columns]

    def _map_if_missing(target: str, candidates: List[str]) -> None:
        if target in normalized.columns:
            return
        for candidate in candidates:
            if candidate in normalized.columns:
                normalized.rename(columns={candidate: target}, inplace=True)
                return

    _map_if_missing("close", ["adj_close", "adjusted_close", "close_price", "stck_clpr"])
    _map_if_missing("high", ["high_price", "stck_hgpr"])
    _map_if_missing("low", ["low_price", "stck_lwpr"])
    _map_if_missing("volume", ["vol", "trade_volume", "acml_vol", "acml_volm"])
    return normalized


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            cleaned = value.replace(",", "").strip()
            if cleaned == "":
                return default
            return float(cleaned)
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _flow_contract_state(
    *,
    foreign_df: Optional[pd.DataFrame],
    inst_df: Optional[pd.DataFrame],
    flow_result: Dict[str, Any],
) -> Tuple[bool, Optional[float], Optional[float], str]:
    foreign_ratio_raw = flow_result.get("foreign_20_ratio")
    inst_ratio_raw = flow_result.get("inst_20_ratio")
    has_foreign_ratio = foreign_ratio_raw is not None
    has_inst_ratio = inst_ratio_raw is not None

    if has_foreign_ratio or has_inst_ratio:
        foreign_ratio = _safe_float(foreign_ratio_raw, 0.0) if has_foreign_ratio else None
        inst_ratio = _safe_float(inst_ratio_raw, 0.0) if has_inst_ratio else None
        return False, foreign_ratio, inst_ratio, "ratio_available"

    if foreign_df is None or inst_df is None:
        return True, None, None, "provider_missing"
    return True, None, None, "ratio_missing"


def _extract_derived_metrics(derived_row: Dict[str, Any]) -> Dict[str, float]:
    features = derived_row.get("features_json") if isinstance(derived_row.get("features_json"), dict) else {}

    close = _safe_float(derived_row.get("close"), 0.0)
    ma50 = _safe_float(derived_row.get("ma50"), 0.0)
    ma150 = _safe_float(derived_row.get("ma150"), 0.0)
    ma200 = _safe_float(derived_row.get("ma200"), 0.0)
    trend_checks = [
        close > ma50 > 0,
        ma50 > ma150 > 0,
        ma150 > ma200 > 0,
        close > ma200 > 0,
    ]
    trend_score = float(sum(1 for check in trend_checks if check) * 25.0)

    hi_52w = _safe_float(derived_row.get("hi_52w"), _safe_float(features.get("hi_52w"), 0.0))
    pullback_pct = 0.0
    if hi_52w > 0 and close > 0:
        pullback_pct = max(0.0, (hi_52w - close) / hi_52w)
    else:
        pullback_pct = _safe_float(features.get("pullback_pct"), 0.0)

    return {
        "rs_pctile": _safe_float(derived_row.get("rs_percentile"), _safe_float(features.get("rs_percentile"), 0.0)),
        "vcp_score": _safe_float(derived_row.get("vcp_score"), _safe_float(features.get("vcp_score"), 0.0)),
        "atr_pct": _safe_float(derived_row.get("atr_pct"), _safe_float(features.get("atr_pct"), 0.0)),
        "trend_score": trend_score,
        "pullback_pct": pullback_pct,
    }


def _sync_item_and_meta_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(item)
    out["code"] = str(out.get("code") or "").zfill(6)
    meta = dict(out.get("meta") or {})
    meta["code"] = out["code"]
    if not str(out.get("name") or "").strip() and str(meta.get("name") or "").strip():
        out["name"] = str(meta.get("name") or "")

    for key in (
        "as_of",
        "rs_pctile",
        "vcp_score",
        "atr_pct",
        "trend_score",
        "pullback_pct",
        "flow_score",
        "tech_score",
        "final_score",
        "foreign_20_ratio",
        "inst_20_ratio",
    ):
        if key in out and out.get(key) is not None:
            meta[key] = out.get(key)
        elif key in meta and meta.get(key) is not None:
            out[key] = meta.get(key)

    out["rows"] = _safe_int(out.get("rows", meta.get("rows", 0)), 0)
    meta["rows"] = out["rows"]

    out["rs_pctile"] = _safe_float(out.get("rs_pctile", meta.get("rs_pctile", 0.0)), 0.0)
    out["vcp_score"] = _safe_float(out.get("vcp_score", meta.get("vcp_score", 0.0)), 0.0)
    out["atr_pct"] = _safe_float(out.get("atr_pct", meta.get("atr_pct", 0.0)), 0.0)
    out["trend_score"] = _safe_float(out.get("trend_score", meta.get("trend_score", 0.0)), 0.0)
    out["pullback_pct"] = _safe_float(out.get("pullback_pct", meta.get("pullback_pct", 0.0)), 0.0)
    out["flow_score"] = _safe_float(out.get("flow_score", meta.get("flow_score", 0.0)), 0.0)
    out["tech_score"] = _safe_float(out.get("tech_score", meta.get("tech_score", out.get("score", 0.0))), 0.0)
    out["final_score"] = _safe_float(out.get("final_score", meta.get("final_score", out.get("score", 0.0))), 0.0)
    flow_missing = bool(out.get("flow_missing") or meta.get("flow_missing"))
    if flow_missing:
        out["foreign_20_ratio"] = out.get("foreign_20_ratio", meta.get("foreign_20_ratio"))
        out["inst_20_ratio"] = out.get("inst_20_ratio", meta.get("inst_20_ratio"))
    else:
        out["foreign_20_ratio"] = _safe_float(out.get("foreign_20_ratio", meta.get("foreign_20_ratio", 0.0)), 0.0)
        out["inst_20_ratio"] = _safe_float(out.get("inst_20_ratio", meta.get("inst_20_ratio", 0.0)), 0.0)

    for key in (
        "rs_pctile",
        "vcp_score",
        "atr_pct",
        "trend_score",
        "pullback_pct",
        "flow_score",
        "tech_score",
        "final_score",
        "foreign_20_ratio",
        "inst_20_ratio",
    ):
        meta[key] = out[key]

    reject_reasons = list(out.get("reject_reasons", []) or meta.get("reject_reasons", []) or [])
    if flow_missing:
        if "flow_data_missing" not in reject_reasons:
            reject_reasons.append("flow_data_missing")
        if "flow_weight_disabled" not in reject_reasons:
            reject_reasons.append("flow_weight_disabled")
    else:
        reject_reasons = [
            reason
            for reason in reject_reasons
            if str(reason) not in {"flow_data_missing", "flow_weight_disabled"}
        ]
    out["reject_reasons"] = list(dict.fromkeys(reject_reasons))
    meta["reject_reasons"] = out["reject_reasons"]

    out["meta"] = meta
    return out


def _enrich_watchlist_rows(
    *,
    engine: Engine,
    env: str,
    as_of: date,
    rows: List[Dict[str, Any]],
    flow_provider: Optional[FlowProvider],
    ohlcv_provider: Any,
    flow_window: int,
    tech_weight: float,
    flow_weight: float,
    trend_weight: float,
) -> List[Dict[str, Any]]:
    if not rows:
        return []

    normalized_rows = [_sync_item_and_meta_fields(dict(row)) for row in rows]
    symbols = [str(row.get("code") or "").zfill(6) for row in normalized_rows if row.get("code")]

    derived_repo = DerivedMinerviniRepo(engine)
    derived_rows = derived_repo.load_for_as_of(env=env, as_of=as_of, symbols=symbols)
    derived_map = {str(row.get("symbol") or "").zfill(6): row for row in derived_rows}
    flow_rows: List[Dict[str, Any]] = []

    enriched: List[Dict[str, Any]] = []
    for item in normalized_rows:
        out = dict(item)
        code = str(out.get("code") or "").zfill(6)
        meta = dict(out.get("meta") or {})
        reject_reasons = list(out.get("reject_reasons", []) or meta.get("reject_reasons", []) or [])

        derived_row = derived_map.get(code)
        if derived_row is None:
            if "derived_missing" not in reject_reasons:
                reject_reasons.append("derived_missing")
            meta["derived_missing"] = True
        else:
            metrics = _extract_derived_metrics(derived_row)
            for key, value in metrics.items():
                out[key] = value
                meta[key] = value
            meta["derived_missing"] = False

        if flow_provider is not None:
            flow_weight_effective = float(flow_weight)
            tech_weight_effective = float(tech_weight)
            trend_weight_effective = float(trend_weight)
            foreign_df: Optional[pd.DataFrame] = None
            inst_df: Optional[pd.DataFrame] = None
            ohlcv_df = pd.DataFrame()
            try:
                ohlcv_raw, _ = ohlcv_provider(code, count=max(int(flow_window) + 10, 80))
                ohlcv_df = _normalize_ohlcv_columns(ohlcv_raw if ohlcv_raw is not None else pd.DataFrame())
            except Exception:
                ohlcv_df = pd.DataFrame()

            try:
                foreign_df, inst_df = flow_provider(code, as_of, int(flow_window))
            except Exception:
                logger.warning("[FLOW][WARN] provider exception code=%s as_of=%s", code, as_of, exc_info=True)
                if "flow_provider_error" not in reject_reasons:
                    reject_reasons.append("flow_provider_error")

            flow_result = calculate_flow_score(
                code=code,
                ohlcv_df=ohlcv_df if ohlcv_df is not None else pd.DataFrame(),
                foreign_df=foreign_df,
                inst_df=inst_df,
                window=int(flow_window),
            )
            flow_score_norm = _safe_float(flow_result.get("flow_score"), 0.0)
            tech_score_val = _safe_float(out.get("tech_score", 0.0), 0.0)

            flow_missing, foreign_ratio, inst_ratio, flow_missing_reason = _flow_contract_state(
                foreign_df=foreign_df,
                inst_df=inst_df,
                flow_result=flow_result,
            )
            if flow_missing:
                flow_weight_effective = 0.0
                tech_weight_effective = 1.0
                trend_weight_effective = 0.0
                logger.warning(
                    "[FLOW][WARN] flow missing -> non_blocking code=%s as_of=%s reason=%s",
                    code,
                    as_of,
                    flow_missing_reason,
                )
            meta["flow_missing"] = bool(flow_missing)
            meta["flow_missing_reason"] = flow_missing_reason
            out["flow_missing"] = bool(flow_missing)
            out["flow_pass"] = True

            out["flow_score"] = flow_score_norm
            out["foreign_20_ratio"] = foreign_ratio
            out["inst_20_ratio"] = inst_ratio
            base_final_score = calculate_final_score(
                tech_score=tech_score_val,
                flow_score=flow_score_norm * 100.0,
                tech_weight=tech_weight_effective,
                flow_weight=flow_weight_effective,
            )
            out["final_score"] = float(base_final_score + (_safe_float(out.get("trend_score"), 0.0) * trend_weight_effective))
            meta["weights_effective"] = {
                "tech_weight": tech_weight_effective,
                "flow_weight": flow_weight_effective,
                "trend_weight": trend_weight_effective,
            }
            meta["formula"] = (
                "score_final = "
                f"{tech_weight_effective:.4f}*score_tech + "
                f"{flow_weight_effective:.4f}*score_flow + "
                f"{trend_weight_effective:.4f}*score_trend"
            )
            flow_rows.append(
                {
                    "as_of": to_date(as_of),
                    "symbol": code,
                    "flow_score": flow_score_norm,
                    "foreign_20_ratio": foreign_ratio,
                    "inst_20_ratio": inst_ratio,
                    "flow_missing": bool(flow_missing),
                    "source": "watchlist_flow_provider",
                    "features_json": {
                        "flow_missing_reason": flow_missing_reason,
                        "weights_effective": meta.get("weights_effective", {}),
                    },
                }
            )

        if _safe_float(out.get("tech_score"), 0.0) <= 0.0:
            rs_pctile = _safe_float(out.get("rs_pctile"), 0.0)
            vcp_score = _safe_float(out.get("vcp_score"), 0.0)
            trend_score = _safe_float(out.get("trend_score"), 0.0)
            pullback_pct = _safe_float(out.get("pullback_pct"), 0.0)
            atr_pct = _safe_float(out.get("atr_pct"), 0.0)
            pullback_score = max(0.0, min(100.0, 100.0 - (pullback_pct * 400.0)))
            atr_score = max(0.0, min(100.0, 100.0 - abs(atr_pct - 0.04) * 1000.0))
            out["tech_score"] = (
                rs_pctile * 0.40
                + vcp_score * 0.25
                + trend_score * 0.20
                + pullback_score * 0.10
                + atr_score * 0.05
            )

        out["reject_reasons"] = list(dict.fromkeys(reject_reasons))
        meta["reject_reasons"] = out["reject_reasons"]
        out["meta"] = meta
        out = _sync_item_and_meta_fields(out)
        enriched.append(out)

    enriched.sort(key=lambda row: _safe_float(row.get("final_score", row.get("score", 0.0)), 0.0), reverse=True)
    for idx, row in enumerate(enriched, start=1):
        row["rank"] = _safe_int(row.get("rank") or idx, idx)
        row.setdefault("rank_final30", idx)
        row.setdefault("rank_top50", _safe_int(row.get("rank_top50"), 0))
        row.setdefault("rank_pool120", _safe_int(row.get("rank_pool120"), 0))

    if flow_rows:
        try:
            DerivedFlowRepo(engine).upsert_rows(env=env, rows=flow_rows)
        except Exception:
            logger.warning("[FLOW][DB][UPSERT_FAIL] env=%s as_of=%s rows=%s", env, as_of, len(flow_rows), exc_info=True)
    return enriched


class WatchlistBuilder:
    """단일 파이프라인으로 universe -> pool120 -> top50 -> final30 생성."""

    def __init__(
        self,
        *,
        ohlcv_provider: Any,
        minervini_config: Dict[str, Any],
        pooln: int = 120,
        topk: int = 50,
        finaln: int = 30,
        min_price: float = 2000.0,
        liq_days: int = 20,
        min_rows: int = 30,
        flow_provider: Optional[FlowProvider] = None,
        flow_window: int = 20,
        tech_weight: float = 0.7,
        flow_weight: float = 0.3,
        trend_weight: float = 0.0,
    ):
        self.ohlcv_provider = ohlcv_provider
        self.minervini_config = minervini_config
        self.pooln = pooln
        self.topk = topk
        self.finaln = finaln
        self.min_price = min_price
        self.liq_days = liq_days
        self.min_rows = min_rows
        self.flow_provider = flow_provider
        self.flow_window = flow_window
        self.tech_weight = tech_weight
        self.flow_weight = flow_weight
        self.trend_weight = trend_weight
        self.last_bundle: Dict[str, Any] = {}

    def build(self, *, members: List[Dict[str, Any]], as_of: date) -> List[Dict[str, Any]]:
        logger.info(
            "[WATCHLIST][BUILD][START] as_of=%s members=%s pooln=%s topk=%s finaln=%s",
            as_of,
            len(members),
            self.pooln,
            self.topk,
            self.finaln,
        )

        pool120, universe_scored = self._stage_a_liquidity_filter(members, as_of)
        top50 = self._stage_b_strategy_scoring(pool120, as_of)
        final30 = self._stage_c_flow_final(top50, as_of)

        contract_failures: List[str] = []
        if len(universe_scored) <= 0:
            contract_failures.append("universe_scored_empty")
        if len(top50) <= 0:
            contract_failures.append("top50_empty")
        if len(pool120) <= 0:
            contract_failures.append("pool120_empty")

        degrade_meta = {
            "used": False,
            "requested_finaln": int(self.finaln),
            "final_count": len(final30),
            "fill_sources": {"top50": 0, "pool120": 0},
            "missing_after_fill": max(0, int(self.finaln) - len(final30)),
        }
        if len(final30) < self.finaln:
            logger.warning(
                "[WATCHLIST][PIPELINE][C_FINAL30][DEGRADE] too small: %s < %s -> fill from B_TOP50 then A_POOL120",
                len(final30),
                self.finaln,
            )
            final30, degrade_meta = self._degrade_fill_final_rows(
                final_rows=final30,
                top50_rows=top50,
                pool120_rows=pool120,
                target_n=self.finaln,
            )

        if len(final30) != int(self.finaln):
            contract_failures.append(f"final30_count_mismatch:{len(final30)}!={int(self.finaln)}")

        allow_degrade = _env_bool("PB1_WATCHLIST_ALLOW_DEGRADE", False)
        if contract_failures and not allow_degrade:
            raise RuntimeError("WATCHLIST_PIPELINE_CONTRACT_FAILED: " + ",".join(contract_failures))

        shortage_reason = ""
        if len(final30) != int(self.finaln):
            shortage_reason = ";".join(contract_failures) or f"pipeline_shortage:{len(final30)}<{int(self.finaln)}"

        degrade_meta = {
            **degrade_meta,
            "enabled": bool(degrade_meta.get("used") or bool(contract_failures)),
            "reason": shortage_reason,
            "disabled_features": ["flow"] if any("flow" in reason for reason in contract_failures) else [],
        }

        reject_counter = Counter()
        for item in universe_scored:
            for reason in item.get("reject_reasons", []):
                reject_counter[reason] += 1

        self.last_bundle = {
            "as_of": as_of,
            "weights": {
                "tech_weight": self.tech_weight,
                "flow_weight": self.flow_weight,
                "trend_weight": self.trend_weight,
            },
            "weights_effective": {
                "tech_weight": self.tech_weight,
                "flow_weight": self.flow_weight,
                "trend_weight": self.trend_weight,
            },
            "formula": (
                "score_final = "
                f"{self.tech_weight:.4f}*score_tech + "
                f"{self.flow_weight:.4f}*score_flow + "
                f"{self.trend_weight:.4f}*score_trend"
            ),
            "universe_scored": universe_scored,
            "pool120": pool120,
            "top50": top50,
            "final30": final30,
            "reject_summary": dict(reject_counter),
            "final_count": len(final30),
            "requested_finaln": int(self.finaln),
            "degrade": degrade_meta,
            "shortage_reason": shortage_reason,
            "contract_failures": contract_failures,
        }

        logger.info(
            "[WATCHLIST][BUILD][DONE] as_of=%s pool120=%s top50=%s final30=%s",
            as_of,
            len(pool120),
            len(top50),
            len(final30),
        )
        return final30

    def _degrade_fill_final_rows(
        self,
        *,
        final_rows: List[Dict[str, Any]],
        top50_rows: List[Dict[str, Any]],
        pool120_rows: List[Dict[str, Any]],
        target_n: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        filled: List[Dict[str, Any]] = [dict(row) for row in final_rows]
        seen_codes = {str(row.get("code") or "").zfill(6) for row in filled}
        fill_sources = {"top50": 0, "pool120": 0}

        def _as_final_row(src: Dict[str, Any], source_name: str) -> Dict[str, Any]:
            row = dict(src)
            reasons = list(row.get("reject_reasons", []) or [])
            reasons.append(f"degrade_fill_from_{source_name}")
            row["reject_reasons"] = reasons
            row["flow_score"] = float(row.get("flow_score", 0.0) or 0.0)
            row["foreign_20_ratio"] = float(row.get("foreign_20_ratio", 0.0) or 0.0)
            row["inst_20_ratio"] = float(row.get("inst_20_ratio", 0.0) or 0.0)
            tech_score = float(row.get("tech_score", 0.0) or 0.0)
            fallback_score = float(row.get("score", 0.0) or 0.0)
            row["final_score"] = float(row.get("final_score", tech_score if tech_score > 0 else fallback_score) or 0.0)
            return self._normalize_item(row, score_key="final_score", rank_key="final_rank")

        for src in top50_rows:
            if len(filled) >= target_n:
                break
            code = str(src.get("code") or "").zfill(6)
            if not code or code in seen_codes:
                continue
            filled.append(_as_final_row(src, "top50"))
            seen_codes.add(code)
            fill_sources["top50"] += 1

        for src in pool120_rows:
            if len(filled) >= target_n:
                break
            code = str(src.get("code") or "").zfill(6)
            if not code or code in seen_codes:
                continue
            filled.append(_as_final_row(src, "pool120"))
            seen_codes.add(code)
            fill_sources["pool120"] += 1

        for idx, item in enumerate(filled, start=1):
            item["final_rank"] = idx
            item["rank"] = idx
            item["rank_final30"] = idx

        degrade_meta = {
            "used": True,
            "requested_finaln": int(target_n),
            "final_count": len(filled),
            "fill_sources": fill_sources,
            "missing_after_fill": max(0, int(target_n) - len(filled)),
        }
        logger.warning(
            "[WATCHLIST][PIPELINE][C_FINAL30][DEGRADE] filled final=%s requested=%s fill_top50=%s fill_pool120=%s",
            len(filled),
            target_n,
            fill_sources["top50"],
            fill_sources["pool120"],
        )
        return filled[:target_n], degrade_meta

    def _base_item(self, code: str, *, name: str = "", as_of: Optional[date] = None) -> Dict[str, Any]:
        return {
            "as_of": as_of.isoformat() if isinstance(as_of, date) else "",
            "code": code,
            "name": name,
            "rank": None,
            "score": None,
            "liq_avg": 0.0,
            "last_close": 0.0,
            "rows": 0,
            "rs_pctile": 0.0,
            "vcp_score": 0.0,
            "pullback_pct": 0.0,
            "trend_score": 0.0,
            "atr_pct": 0.0,
            "foreign_20_ratio": 0.0,
            "inst_20_ratio": 0.0,
            "flow_score": 0.0,
            "tech_score": 0.0,
            "final_score": 0.0,
            "reject_reasons": [],
            "meta": {},
        }

    def _normalize_item(self, item: Dict[str, Any], *, score_key: str, rank_key: str) -> Dict[str, Any]:
        score_val = float(item.get(score_key, 0.0) or 0.0)
        rank_val = item.get(rank_key) or item.get("rank")
        reject_reasons = list(item.get("reject_reasons", []) or [])
        filters_passed = list(item.get("filters_passed", []) or [])
        rs_min_pctile = float(self.minervini_config.get("rs_min_pctile", RS_MIN_PCTILE))
        if rs_min_pctile <= 1.0:
            rs_min_pctile = rs_min_pctile * 100.0
        vcp_min_score = float(self.minervini_config.get("vcp_min_score", 70.0) or 70.0)

        scores = {
            "rs_pctile": float(item.get("rs_pctile", 0.0) or 0.0),
            "vcp_score": float(item.get("vcp_score", 0.0) or 0.0),
            "trend_template": 1 if float(item.get("trend_score", 0.0) or 0.0) >= 100.0 else 0,
            "liquidity_rank": int(item.get("pool_rank", 0) or item.get("rank_pool120", 0) or 0),
            "pullback_score": max(0.0, min(100.0, 100.0 - float(item.get("pullback_pct", 0.0) or 0.0) * 400.0)),
            "foreign_score": max(0.0, min(100.0, float(item.get("foreign_20_ratio", 0.0) or 0.0) * 100.0)),
            "inst_score": max(0.0, min(100.0, float(item.get("inst_20_ratio", 0.0) or 0.0) * 100.0)),
        }

        reasons_raw = item.get("reasons")
        reasons: Dict[str, Any]
        if reasons_raw is None:
            reasons = {}
        elif isinstance(reasons_raw, dict):
            reasons = dict(reasons_raw)
        elif isinstance(reasons_raw, list):
            if all(isinstance(x, str) for x in reasons_raw):
                reasons = {"bullets": list(reasons_raw)}
            elif all(isinstance(x, dict) for x in reasons_raw):
                merged: Dict[str, Any] = {}
                can_merge = True
                for entry in reasons_raw:
                    for key, value in entry.items():
                        if key in merged:
                            can_merge = False
                            break
                        merged[key] = value
                    if not can_merge:
                        break
                reasons = merged if can_merge else {"items": list(reasons_raw)}
            else:
                reasons = {"raw": str(reasons_raw)}
        else:
            reasons = {"raw": str(reasons_raw)}

        passed = list(reasons.get("passed", []) or [])
        failed = list(reasons.get("failed", []) or [])
        notes = dict(reasons.get("notes", {}) or {})

        if scores["rs_pctile"] >= rs_min_pctile:
            passed.append(f"RS>={rs_min_pctile:.0f}")
        if scores["vcp_score"] >= vcp_min_score:
            passed.append(f"VCP>={vcp_min_score:.0f}")
        if scores["trend_template"] == 1:
            passed.append("Trend=Yes")
        if float(item.get("flow_score", 0.0) or 0.0) > 0.0:
            passed.append("Flow=Positive")

        failed.extend(reject_reasons)
        passed = list(dict.fromkeys(passed))
        failed = list(dict.fromkeys(failed))

        notes.setdefault("rs_pctile", scores["rs_pctile"])
        notes.setdefault("vcp_score", scores["vcp_score"])
        notes.setdefault("foreign_net_20d", float(item.get("foreign_net_20d", item.get("foreign_20_ratio", 0.0)) or 0.0))
        notes.setdefault("inst_net_20d", float(item.get("inst_net_20d", item.get("inst_20_ratio", 0.0)) or 0.0))

        reasons = {
            **reasons,
            "passed": passed,
            "failed": failed,
            "notes": notes,
        }

        filters_failed = list(item.get("filters_failed", []) or reasons.get("failed", []) or reject_reasons)
        if not filters_passed:
            if float(item.get("liq_avg", 0.0) or 0.0) > 0.0:
                filters_passed.append("A_POOL120")
            if float(item.get("tech_score", 0.0) or 0.0) > 0.0:
                filters_passed.append("B_TOP50")
            if float(item.get("final_score", 0.0) or 0.0) > 0.0:
                filters_passed.append("C_FINAL30")
        score_liq = float(item.get("liq_avg", 0.0) or 0.0)
        score_tech = float(item.get("tech_score", 0.0) or 0.0)
        score_flow = float(item.get("flow_score", 0.0) or 0.0)
        score_final = float(item.get("final_score", score_val) or 0.0)
        rank_pool120 = int(item.get("pool_rank", 0) or 0)
        rank_top50 = int(item.get("top50_rank", 0) or 0)
        rank_final30 = int(item.get("final_rank", 0) or 0)
        meta = {
            "as_of": str(item.get("as_of") or ""),
            "name": str(item.get("name") or ""),
            "liq_avg": float(item.get("liq_avg", 0.0) or 0.0),
            "last_close": float(item.get("last_close", 0.0) or 0.0),
            "rows": int(item.get("rows", 0) or 0),
            "rs_pctile": float(item.get("rs_pctile", 0.0) or 0.0),
            "vcp_score": float(item.get("vcp_score", 0.0) or 0.0),
            "pullback_pct": float(item.get("pullback_pct", 0.0) or 0.0),
            "trend_score": float(item.get("trend_score", 0.0) or 0.0),
            "atr_pct": float(item.get("atr_pct", 0.0) or 0.0),
            "foreign_20_ratio": float(item.get("foreign_20_ratio", 0.0) or 0.0),
            "inst_20_ratio": float(item.get("inst_20_ratio", 0.0) or 0.0),
            "flow_score": float(item.get("flow_score", 0.0) or 0.0),
            "tech_score": float(item.get("tech_score", 0.0) or 0.0),
            "final_score": float(item.get("final_score", 0.0) or 0.0),
            "weights": {
                "tech_weight": self.tech_weight,
                "flow_weight": self.flow_weight,
            },
            "reject_reasons": reject_reasons,
            "reasons": reasons,
            "filters_passed": filters_passed,
            "filters_failed": filters_failed,
            "rank_pool120": rank_pool120,
            "rank_top50": rank_top50,
            "rank_final30": rank_final30,
            "score_liq": score_liq,
            "score_tech": score_tech,
            "score_flow": score_flow,
            "score_final": score_final,
            "scores": scores,
            **(item.get("meta") or {}),
        }
        return {
            "as_of": str(item.get("as_of") or ""),
            "code": str(item.get("code") or "").zfill(6),
            "name": str(item.get("name") or ""),
            "rank": int(rank_val) if rank_val else 0,
            "score": score_val,
            "meta": meta,
            "liq_avg": meta["liq_avg"],
            "last_close": meta["last_close"],
            "rows": meta["rows"],
            "rs_pctile": meta["rs_pctile"],
            "vcp_score": meta["vcp_score"],
            "pullback_pct": meta["pullback_pct"],
            "trend_score": meta["trend_score"],
            "atr_pct": meta["atr_pct"],
            "foreign_20_ratio": meta["foreign_20_ratio"],
            "inst_20_ratio": meta["inst_20_ratio"],
            "flow_score": meta["flow_score"],
            "tech_score": meta["tech_score"],
            "final_score": meta["final_score"],
            "reject_reasons": reject_reasons,
            "reasons": reasons,
            "scores": scores,
            "filters_passed": filters_passed,
            "filters_failed": filters_failed,
            "rank_pool120": rank_pool120,
            "rank_top50": rank_top50,
            "rank_final30": rank_final30,
            "score_liq": score_liq,
            "score_tech": score_tech,
            "score_flow": score_flow,
            "score_final": score_final,
        }

    def _stage_a_liquidity_filter(self, members: List[Dict[str, Any]], as_of: date) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        excluded_rows = 0
        excluded_price = 0
        excluded_nan = 0

        candidates: List[Dict[str, Any]] = []
        universe_items: List[Dict[str, Any]] = []
        total = len(members)
        progress_every = max(1, total // 10)
        ts0 = time.monotonic()

        for idx, m in enumerate(members, start=1):
            code = str(m.get("code") or "").zfill(6)
            if idx == 1 or idx % progress_every == 0 or idx == total:
                logger.info(
                    "[WATCHLIST][PIPELINE][A_POOL120][PROGRESS] processed=%s/%s candidates=%s excluded(rows=%s,price=%s,nan=%s) elapsed=%.1fs",
                    idx,
                    total,
                    len(candidates),
                    excluded_rows,
                    excluded_price,
                    excluded_nan,
                    time.monotonic() - ts0,
                )
            if not code:
                continue

            item = self._base_item(code, name=str(m.get("name") or ""), as_of=as_of)

            try:
                df, _meta = self.ohlcv_provider(code, count=max(self.min_rows, self.liq_days + 30, 260))
            except Exception as exc:
                item["reject_reasons"].append("ohlcv_fetch_error")
                excluded_rows += 1
                logger.debug("[WATCHLIST][PIPELINE][A_POOL120][OHLCV_FAIL] code=%s err=%s", code, exc)
                universe_items.append(item)
                continue

            if df is None or df.empty:
                item["reject_reasons"].append("ohlcv_empty")
                excluded_rows += 1
                universe_items.append(item)
                continue

            df = _normalize_ohlcv_columns(df)

            rows = len(df)
            item["rows"] = rows
            if rows < self.min_rows:
                item["reject_reasons"].append("rows_below_min")
                excluded_rows += 1
                universe_items.append(item)
                continue

            if "close" not in df.columns or "volume" not in df.columns:
                item["reject_reasons"].append("missing_close_or_volume")
                excluded_nan += 1
                universe_items.append(item)
                continue

            last_close = df["close"].iloc[-1]
            if pd.isna(last_close):
                item["reject_reasons"].append("last_close_nan")
                excluded_nan += 1
                universe_items.append(item)
                continue
            if float(last_close) < self.min_price:
                item["reject_reasons"].append("price_below_min")
                excluded_price += 1
                item["last_close"] = float(last_close)
                universe_items.append(item)
                continue

            recent = df.tail(self.liq_days)
            liq_avg = (recent["close"] * recent["volume"]).mean()
            if pd.isna(liq_avg):
                item["reject_reasons"].append("liq_nan")
                excluded_nan += 1
                universe_items.append(item)
                continue

            item["liq_avg"] = float(liq_avg)
            item["last_close"] = float(last_close)
            item["meta"] = {"as_of": as_of.isoformat()}
            candidates.append(item)
            universe_items.append(item)

        candidates.sort(key=lambda x: x.get("liq_avg", 0.0), reverse=True)
        pool120 = candidates[: self.pooln]

        keep_codes = {x["code"] for x in pool120}
        for idx, item in enumerate(pool120, start=1):
            item["pool_rank"] = idx

        for item in universe_items:
            if item["code"] not in keep_codes and not item.get("reject_reasons"):
                item.setdefault("reject_reasons", []).append("not_in_pool120")

        normalized_pool = [self._normalize_item(item, score_key="liq_avg", rank_key="pool_rank") for item in pool120]
        normalized_universe = [self._normalize_item(item, score_key="liq_avg", rank_key="pool_rank") for item in universe_items]

        logger.info(
            "[WATCHLIST][PIPELINE][A_POOL120] kept=%s universe=%s excluded_rows=%s excluded_price=%s excluded_nan=%s",
            len(normalized_pool),
            len(normalized_universe),
            excluded_rows,
            excluded_price,
            excluded_nan,
        )
        return normalized_pool, normalized_universe

    def _stage_b_strategy_scoring(self, pool120: List[Dict[str, Any]], as_of: date) -> List[Dict[str, Any]]:
        if not pool120:
            logger.warning("[WATCHLIST][PIPELINE][B_TOP50] kept=0 from=0")
            return []

        try:
            bench_df, _ = self.ohlcv_provider(RS_BENCHMARK, count=max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, 260) + 10)
            bench_df = _normalize_ohlcv_columns(bench_df if bench_df is not None else pd.DataFrame())
            if bench_df is None or bench_df.empty or "close" not in bench_df.columns:
                raise ValueError(f"benchmark {RS_BENCHMARK} empty")
            bench_close = bench_df["close"]
        except Exception as exc:
            logger.error("[WATCHLIST][PIPELINE][B_TOP50][BENCH_FAIL] err=%s", exc)
            bench_close = None

        rs_min_pctile = float(self.minervini_config.get("rs_min_pctile", RS_MIN_PCTILE))
        if rs_min_pctile <= 1.0:
            rs_min_pctile = rs_min_pctile * 100.0
        vcp_min_score = float(self.minervini_config.get("vcp_min_score", 70.0) or 70.0)

        scored: List[Dict[str, Any]] = []
        total = len(pool120)
        progress_every = max(1, total // 10)
        ts0 = time.monotonic()
        for idx, cand in enumerate(pool120, start=1):
            if idx == 1 or idx % progress_every == 0 or idx == total:
                logger.info(
                    "[WATCHLIST][PIPELINE][B_TOP50][PROGRESS] processed=%s/%s scored=%s elapsed=%.1fs",
                    idx,
                    total,
                    len(scored),
                    time.monotonic() - ts0,
                )
            code = cand["code"]
            item = dict(cand)
            reject_reasons = list(item.get("reject_reasons", []))

            try:
                df, _ = self.ohlcv_provider(code, count=max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, 260) + 10)
                df = _normalize_ohlcv_columns(df if df is not None else pd.DataFrame())
            except Exception:
                reject_reasons.append("ohlcv_fetch_error_stage_b")
                item["reject_reasons"] = reject_reasons
                continue

            if df is None or df.empty or len(df) < 120:
                reject_reasons.append("insufficient_rows_stage_b")
                item["reject_reasons"] = reject_reasons
                continue

            rs_pctile = self._compute_rs_percentile(df["close"], bench_close)
            vcp_score = self._compute_vcp_score(df)
            pullback_pct = self._compute_pullback_pct(df)
            trend_score = self._compute_trend_score(df)
            atr_pct = self._compute_atr_pct(df)

            pullback_score = max(0.0, min(100.0, 100.0 - (pullback_pct * 400.0)))
            atr_score = max(0.0, min(100.0, 100.0 - abs(atr_pct - 0.04) * 1000.0))
            tech_score = (
                rs_pctile * 0.40
                + vcp_score * 0.25
                + trend_score * 0.20
                + pullback_score * 0.10
                + atr_score * 0.05
            )

            if rs_pctile < rs_min_pctile:
                reject_reasons.append("rs_below_min")
            if vcp_score < vcp_min_score:
                reject_reasons.append("vcp_below_min")
            if trend_score < 100.0:
                reject_reasons.append("trend_template_fail")

            item.update(
                {
                    "rs_pctile": float(rs_pctile),
                    "vcp_score": float(vcp_score),
                    "pullback_pct": float(pullback_pct),
                    "trend_score": float(trend_score),
                    "atr_pct": float(atr_pct),
                    "tech_score": float(tech_score),
                    "reject_reasons": reject_reasons,
                }
            )
            scored.append(item)

        scored.sort(key=lambda x: x.get("tech_score", 0.0), reverse=True)
        top50 = scored[: self.topk]
        keep_codes = {item["code"] for item in top50}

        for idx, item in enumerate(top50, start=1):
            item["top50_rank"] = idx
        for item in scored[self.topk :]:
            item.setdefault("reject_reasons", []).append("not_in_top50")

        normalized = [self._normalize_item(item, score_key="tech_score", rank_key="top50_rank") for item in top50]

        logger.info("[WATCHLIST][PIPELINE][B_TOP50] kept=%s from=%s", len(normalized), len(pool120))
        return normalized

    def _stage_c_flow_final(self, top50: List[Dict[str, Any]], as_of: date) -> List[Dict[str, Any]]:
        if not top50:
            logger.warning("[WATCHLIST][PIPELINE][C_FINAL30] kept=0 from=0")
            return []

        if self.flow_provider is None:
            logger.warning("[WATCHLIST][PIPELINE][C_FINAL30][FLOW] provider missing -> flow weight disabled by item")

        scored: List[Dict[str, Any]] = []
        total = len(top50)
        progress_every = max(1, total // 10)
        ts0 = time.monotonic()
        for idx, cand in enumerate(top50, start=1):
            if idx == 1 or idx % progress_every == 0 or idx == total:
                logger.info(
                    "[WATCHLIST][PIPELINE][C_FINAL30][PROGRESS] processed=%s/%s scored=%s elapsed=%.1fs",
                    idx,
                    total,
                    len(scored),
                    time.monotonic() - ts0,
                )
            code = cand["code"]
            item = dict(cand)
            reject_reasons = list(item.get("reject_reasons", []))

            try:
                ohlcv_df, _ = self.ohlcv_provider(code, count=max(self.flow_window + 10, 80))
                ohlcv_df = _normalize_ohlcv_columns(ohlcv_df if ohlcv_df is not None else pd.DataFrame())
            except Exception:
                ohlcv_df = pd.DataFrame()

            foreign_df: Optional[pd.DataFrame] = None
            inst_df: Optional[pd.DataFrame] = None
            if self.flow_provider is not None:
                try:
                    foreign_df, inst_df = self.flow_provider(code, as_of, self.flow_window)
                except Exception as exc:
                    logger.debug("[WATCHLIST][PIPELINE][C_FINAL30][FLOW_PROVIDER_FAIL] code=%s err=%s", code, exc)

            flow_weight_effective = self.flow_weight
            tech_weight_effective = self.tech_weight
            trend_weight_effective = self.trend_weight

            flow_result = calculate_flow_score(
                code=code,
                ohlcv_df=ohlcv_df if ohlcv_df is not None else pd.DataFrame(),
                foreign_df=foreign_df,
                inst_df=inst_df,
                window=self.flow_window,
            )

            flow_score_norm = float(flow_result.get("flow_score", 0.0) or 0.0)
            flow_score_100 = flow_score_norm * 100.0
            tech_score = float(item.get("tech_score", 0.0) or 0.0)
            flow_missing, foreign_ratio, inst_ratio, flow_missing_reason = _flow_contract_state(
                foreign_df=foreign_df,
                inst_df=inst_df,
                flow_result=flow_result,
            )
            if flow_missing:
                flow_weight_effective = 0.0
                tech_weight_effective = 1.0
                trend_weight_effective = 0.0

            final_score = calculate_final_score(
                tech_score=tech_score,
                flow_score=flow_score_100,
                tech_weight=tech_weight_effective,
                flow_weight=flow_weight_effective,
            )
            final_score = float(final_score + (float(item.get("trend_score", 0.0) or 0.0) * float(trend_weight_effective)))

            item["flow_missing"] = bool(flow_missing)
            item["flow_missing_reason"] = flow_missing_reason
            item["flow_pass"] = True
            if bool(item.get("flow_missing")):
                if "flow_data_missing" not in reject_reasons:
                    reject_reasons.append("flow_data_missing")
                if "flow_weight_disabled" not in reject_reasons:
                    reject_reasons.append("flow_weight_disabled")
            else:
                reject_reasons = [
                    reason
                    for reason in reject_reasons
                    if str(reason) not in {"flow_data_missing", "flow_weight_disabled"}
                ]

            item.update(
                {
                    "flow_score": flow_score_norm,
                    "foreign_20_ratio": foreign_ratio,
                    "inst_20_ratio": inst_ratio,
                    "final_score": float(final_score),
                    "reject_reasons": reject_reasons,
                    "flow_weight_effective": float(flow_weight_effective),
                    "tech_weight_effective": float(tech_weight_effective),
                }
            )
            meta = dict(item.get("meta") or {})
            meta["weights_effective"] = {
                "tech_weight": float(tech_weight_effective),
                "flow_weight": float(flow_weight_effective),
                "trend_weight": float(trend_weight_effective),
            }
            meta["formula"] = (
                "score_final = "
                f"{float(tech_weight_effective):.4f}*score_tech + "
                f"{float(flow_weight_effective):.4f}*score_flow + "
                f"{float(trend_weight_effective):.4f}*score_trend"
            )
            item["meta"] = meta
            scored.append(item)

        scored.sort(key=lambda x: x.get("final_score", 0.0), reverse=True)
        final30 = scored[: self.finaln]

        for idx, item in enumerate(final30, start=1):
            item["final_rank"] = idx

        normalized_final = [self._normalize_item(item, score_key="final_score", rank_key="final_rank") for item in final30]

        rs_fail = 0
        vcp_fail = 0
        trend_fail = 0
        flow_fail = 0
        for item in scored:
            reasons = list(item.get("reject_reasons", []) or [])
            if "rs_below_min" in reasons:
                rs_fail += 1
            if "vcp_below_min" in reasons:
                vcp_fail += 1
            if "trend_template_fail" in reasons:
                trend_fail += 1
            if any(str(reason).startswith("flow_") for reason in reasons if "missing" not in str(reason)):
                flow_fail += 1

        logger.info(
            "[WATCHLIST][PIPELINE][C_FINAL30] kept=%s rs_fail=%s vcp_fail=%s trend_fail=%s flow_fail=%s from=%s requested=%s",
            len(normalized_final),
            rs_fail,
            vcp_fail,
            trend_fail,
            flow_fail,
            len(top50),
            self.finaln,
        )
        return normalized_final

    def _compute_rs_percentile(self, stock_close: pd.Series, bench_close: Optional[pd.Series]) -> float:
        if bench_close is None or len(stock_close) < RS_LOOKBACK_DAYS or len(bench_close) < RS_LOOKBACK_DAYS:
            return 0.0

        stock_ret = (stock_close.iloc[-1] / stock_close.iloc[-RS_LOOKBACK_DAYS] - 1) * 100
        bench_ret = (bench_close.iloc[-1] / bench_close.iloc[-RS_LOOKBACK_DAYS] - 1) * 100
        rs = stock_ret - bench_ret

        if rs > 20:
            return 90.0
        if rs > 10:
            return 80.0
        if rs > 0:
            return 70.0
        return max(0.0, 50.0 + rs)

    def _compute_vcp_score(self, df: pd.DataFrame) -> float:
        if df is None or df.empty or len(df) < 120:
            return 0.0
        if "high" not in df.columns or "low" not in df.columns:
            return 0.0

        recent_30 = df["high"].tail(30) / df["low"].tail(30) - 1
        prev_90 = df["high"].iloc[-120:-30] / df["low"].iloc[-120:-30] - 1

        recent_vol = float(recent_30.mean())
        prev_vol = float(prev_90.mean())
        if prev_vol == 0:
            return 0.0

        contraction_ratio = recent_vol / prev_vol
        if contraction_ratio < 0.5:
            return 90.0
        if contraction_ratio < 0.7:
            return 80.0
        if contraction_ratio < 0.9:
            return 70.0
        return max(0.0, 100.0 - contraction_ratio * 100.0)

    def _compute_pullback_pct(self, df: pd.DataFrame) -> float:
        if "close" not in df.columns or df.empty:
            return 0.0
        lookback = min(252, len(df))
        high_52w = float(df["close"].tail(lookback).max())
        last_close = float(df["close"].iloc[-1])
        if high_52w <= 0:
            return 0.0
        return max(0.0, (high_52w - last_close) / high_52w)

    def _compute_trend_score(self, df: pd.DataFrame) -> float:
        if "close" not in df.columns or len(df) < 200:
            return 0.0
        close = df["close"]
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1])
        last_close = float(close.iloc[-1])

        checks = [
            last_close > ma20,
            ma20 > ma50,
            ma50 > ma200,
            last_close > ma200,
        ]
        return float(sum(1 for x in checks if x) * 25.0)

    def _compute_atr_pct(self, df: pd.DataFrame, period: int = 14) -> float:
        if df is None or len(df) < period + 1:
            return 0.0
        required = {"high", "low", "close"}
        if not required.issubset(df.columns):
            return 0.0

        high = df["high"]
        low = df["low"]
        close = df["close"]
        prev_close = close.shift(1)

        tr = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        last_close = close.iloc[-1]
        if pd.isna(atr) or pd.isna(last_close) or float(last_close) == 0.0:
            return 0.0
        return float(atr) / float(last_close)


def build_and_save_watchlist(
    *,
    engine: Engine,
    env: str,
    strategy: str,
    as_of: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    minervini_config: Dict[str, Any],
    force_rebuild: bool = False,
    flow_provider: Optional[FlowProvider] = None,
    return_bundle: bool = False,
) -> Any:
    """Watchlist를 생성하고 DB에 저장한다."""
    repo = WatchlistRepo(engine)

    if not force_rebuild:
        existing, _used_as_of = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=as_of,
            allow_latest_fallback=False,
        )
        if existing:
            finaln = _env_int("PB1_WATCHLIST_FINALN", 30)
            if len(existing) < finaln:
                logger.warning(
                    "[WATCHLIST][CACHE][IGNORE] cached too small: %s < %s -> treat as miss",
                    len(existing),
                    finaln,
                )
            else:
                logger.info("[WATCHLIST][CACHE] hit=True as_of=%s members=%s", as_of, len(existing))
                existing = _enrich_watchlist_rows(
                    engine=engine,
                    env=env,
                    as_of=as_of,
                    rows=existing,
                    flow_provider=flow_provider,
                    ohlcv_provider=ohlcv_provider,
                    flow_window=_env_int("FLOW_WINDOW_DAYS", 20),
                    tech_weight=_env_float("WATCHLIST_TECH_WEIGHT", 0.7),
                    flow_weight=_env_float("WATCHLIST_FLOW_WEIGHT", 0.3),
                    trend_weight=_env_float("WATCHLIST_TREND_WEIGHT", 0.0),
                )
                repo.save_watchlist(
                    env=env,
                    strategy=strategy,
                    as_of=as_of,
                    members=existing,
                )
                if return_bundle:
                    return existing, {
                        "as_of": as_of,
                        "weights": {
                            "tech_weight": 0.7,
                            "flow_weight": 0.3,
                            "trend_weight": 0.0,
                        },
                        "weights_effective": {
                            "tech_weight": 0.7,
                            "flow_weight": 0.3,
                            "trend_weight": 0.0,
                        },
                        "formula": "score_final = 0.7000*score_tech + 0.3000*score_flow + 0.0000*score_trend",
                        "universe_scored": [],
                        "pool120": [],
                        "top50": [],
                        "final30": existing,
                        "reject_summary": {"cache_bundle_stage_missing": 1},
                        "shortage_reason": "cache_bundle_stage_missing",
                        "degrade": {
                            "enabled": True,
                            "used": True,
                            "reason": "cache_bundle_stage_missing",
                            "disabled_features": ["stage_snapshot"],
                        },
                    }
                return existing

    builder = WatchlistBuilder(
        ohlcv_provider=ohlcv_provider,
        minervini_config=minervini_config,
        pooln=_env_int("PB1_WATCHLIST_POOLN", 120),
        topk=_env_int("PB1_WATCHLIST_TOPK", 50),
        finaln=_env_int("PB1_WATCHLIST_FINALN", 30),
        min_price=_env_float("PB1_WATCHLIST_MIN_PRICE", 2000.0),
        liq_days=_env_int("PB1_WATCHLIST_LIQ_DAYS", 20),
        min_rows=_env_int("PB1_WATCHLIST_MIN_ROWS", 30),
        flow_provider=flow_provider,
        flow_window=_env_int("FLOW_WINDOW_DAYS", 20),
        tech_weight=_env_float("WATCHLIST_TECH_WEIGHT", 0.7),
        flow_weight=_env_float("WATCHLIST_FLOW_WEIGHT", 0.3),
        trend_weight=_env_float("WATCHLIST_TREND_WEIGHT", 0.0),
    )

    try:
        watchlist = builder.build(members=members, as_of=as_of)
    except Exception as exc:
        logger.error("[WATCHLIST][BUILD][FAIL] err=%s", exc, exc_info=True)
        raise

    watchlist = _enrich_watchlist_rows(
        engine=engine,
        env=env,
        as_of=as_of,
        rows=watchlist,
        flow_provider=flow_provider,
        ohlcv_provider=ohlcv_provider,
        flow_window=_env_int("FLOW_WINDOW_DAYS", 20),
        tech_weight=_env_float("WATCHLIST_TECH_WEIGHT", 0.7),
        flow_weight=_env_float("WATCHLIST_FLOW_WEIGHT", 0.3),
        trend_weight=_env_float("WATCHLIST_TREND_WEIGHT", 0.0),
    )

    repo.save_watchlist(
        env=env,
        strategy=strategy,
        as_of=as_of,
        members=watchlist,
    )

    if return_bundle:
        return watchlist, builder.last_bundle
    return watchlist


def load_today_watchlist_with_fallback(
    *,
    engine: Engine,
    env: str,
    strategy: str,
    today: date,
    members: List[Dict[str, Any]],
    ohlcv_provider: Any,
    minervini_config: Dict[str, Any],
    auto_build_if_empty: bool = True,
    strict_fail_on_empty: bool = False,
) -> tuple[List[Dict[str, Any]], str]:
    """오늘 watchlist를 로드, 없으면 자동 생성 또는 fallback 전략 적용."""
    today = to_date(today)
    repo = WatchlistRepo(engine)
    ttl_days = int(os.getenv("CANDIDATE_POOL_TTL_DAYS", "7"))

    def _load() -> tuple[List[Dict[str, Any]], date | None]:
        rows, used_as_of = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=today,
            allow_latest_fallback=True,
            ttl_days=ttl_days,
        )
        return rows, used_as_of

    rows, used_as_of = _load()
    if rows:
        finaln = _env_int("PB1_WATCHLIST_FINALN", 30)
        if len(rows) < finaln:
            logger.warning("[WATCHLIST][CACHE][IGNORE] cached too small: %s < %s", len(rows), finaln)
        else:
            if used_as_of and used_as_of != today:
                logger.info(
                    "[WATCHLIST][CACHE] hit=True source=watchlist_db_fallback as_of=%s requested=%s count=%s",
                    used_as_of,
                    today,
                    len(rows),
                )
            else:
                logger.info("[WATCHLIST][CACHE] hit=True source=watchlist_db as_of=%s count=%s", today, len(rows))
            return rows, "watchlist_db"

    if auto_build_if_empty:
        logger.warning("[WATCHLIST][AUTO_BUILD] today=%s missing -> building now", today)
        try:
            built = build_and_save_watchlist(
                engine=engine,
                env=env,
                strategy=strategy,
                as_of=today,
                members=members,
                ohlcv_provider=ohlcv_provider,
                minervini_config=minervini_config,
                force_rebuild=True,
            )
            if built:
                rows2, _ = _load()
                if rows2:
                    logger.info("[WATCHLIST][AUTO_BUILD] success count=%s", len(rows2))
                    return rows2, "watchlist_autobuilt"
        except Exception as exc:
            logger.error("[WATCHLIST][AUTO_BUILD][FAIL] err=%s", exc, exc_info=True)

    latest_date = repo.get_latest_watchlist_date(env=env, strategy=strategy)
    if latest_date:
        prev_rows, _ = repo.load_watchlist(
            env=env,
            strategy=strategy,
            as_of=latest_date,
            allow_latest_fallback=False,
        )
        if prev_rows:
            logger.warning("[WATCHLIST][CACHE] hit=True source=prevday as_of=%s", latest_date)
            return prev_rows, "prevday"

    logger.warning("[WATCHLIST][CACHE] miss -> fallback to liquidity pool")
    try:
        builder = WatchlistBuilder(
            ohlcv_provider=ohlcv_provider,
            minervini_config=minervini_config,
            pooln=_env_int("PB1_WATCHLIST_TOPK", 50),
            topk=_env_int("PB1_WATCHLIST_TOPK", 50),
            finaln=_env_int("PB1_WATCHLIST_TOPK", 50),
            min_price=_env_float("PB1_WATCHLIST_MIN_PRICE", 2000.0),
            liq_days=_env_int("PB1_WATCHLIST_LIQ_DAYS", 20),
            min_rows=_env_int("PB1_WATCHLIST_MIN_ROWS", 30),
        )
        fallback, _ = builder._stage_a_liquidity_filter(members, today)
        for idx, item in enumerate(fallback, start=1):
            item["rank"] = idx
        return fallback, "fallback"
    except Exception as exc:
        logger.error("[WATCHLIST][FALLBACK][FAIL] err=%s", exc)
        if strict_fail_on_empty:
            raise RuntimeError(f"Watchlist empty after auto-build: env={env} strategy={strategy} as_of={today}") from exc
        return [], "watchlist_empty"
