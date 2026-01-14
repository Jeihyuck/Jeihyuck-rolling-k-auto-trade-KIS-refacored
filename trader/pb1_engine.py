from __future__ import annotations

import logging
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List

import pandas as pd

from trader.config import (
    CAP_CAP,
    DAILY_CAPITAL,
    KOSDAQ_HARD_STOP_PCT,
    KOSPI_HARD_STOP_PCT,
    PB1_ENTRY_ENABLED,
    PB1_DAY_SL_R,
    PB1_DAY_TP_R,
    PB1_R_FLOOR_PCT,
    PB1_TIME_STOP_DAYS,
    PB1_REQUIRE_VOLUME,
    PB1_MIN_CANDLES,
    PB1_MAX_POSITIONS,
    PB1_MIN_SCORE,
    PB1_USE_RISK_PARITY,
    PB1_MAX_ATR_PCT,
    PB1_MIN_VALUE20,
    PB1_ENTRY_WINDOW_START,
    PB1_ENTRY_OPEN_END,
    PB1_ENTRY_WINDOW_END,
    PB1_EXIT_WINDOW_START,
    PB1_EXIT_WINDOW_END,
    resolve_market_window,
)
from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo, UniverseRepo
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.kis_wrapper import KisAPI
from trader.strategies.pb1_pullback_close import choose_mode, compute_features, score_setup
from trader.time_utils import now_kst
from trader.utils.env import env_bool, parse_env_flag
from trader.window_router import WindowDecision

logger = logging.getLogger(__name__)

_OUTPUT2_LIST_NORMALIZED_LOGGED = False
_OUTPUT2_UNEXPECTED_TYPE_LOGGED = False

UNREALIZED_KEYS = (
    "evlu_pfls_amt",
    "evlu_pfls_smtl_amt",
)
RETURN_PCT_KEYS = (
    "asst_icdc_erng_rt",
    "evlu_pfls_rt",
)
COST_KEYS = (
    "pchs_amt_smtl_amt",
    "pchs_amt",
)
EVAL_KEYS = (
    "scts_evlu_amt",
    "evlu_amt_smtl_amt",
    "tot_evlu_amt",
    "nass_amt",
)
CASH_KEYS = (
    "dnca_tot_amt",
    "nxdy_excc_amt",
    "ord_psbl_cash",
    "evlu_amt_sbst_amt",
)


def _as_first_dict(v: Any) -> Dict[str, Any]:
    """
    KIS 응답에서 output2가 list([dict])로 오는 케이스가 많음.
    dict / list / None 등 어떤 형태든 dict로 정규화.
    """
    global _OUTPUT2_LIST_NORMALIZED_LOGGED
    global _OUTPUT2_UNEXPECTED_TYPE_LOGGED

    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, list):
        if not _OUTPUT2_LIST_NORMALIZED_LOGGED:
            logger.info("[BALANCE] output2 list->dict normalized")
            _OUTPUT2_LIST_NORMALIZED_LOGGED = True
        for item in v:
            if isinstance(item, dict):
                return item
        return {}
    if not _OUTPUT2_UNEXPECTED_TYPE_LOGGED:
        logger.warning("[BALANCE] output2 unexpected type=%s using rows fallback", type(v).__name__)
        _OUTPUT2_UNEXPECTED_TYPE_LOGGED = True
    return {}


def _is_missing(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and value != value)


@dataclass
class CandidateFeature:
    code: str
    market: str
    features: Dict[str, float]
    setup_ok: bool
    reasons: List[str]
    mode: int
    mode_reasons: List[str]
    client_order_key: str | None = None
    planned_qty: int = 0


@dataclass
class RunResult:
    status: str
    notes: str | None = None
    balance_api_calls: int = 0
    balance_cache_hits: int = 0
    balance_tick_cache_hits: int = 0


@dataclass(frozen=True)
class FilterThresholds:
    vol_contraction_max: float
    volu_contraction_max: float
    pullback_min: float
    pullback_max: float
    require_both_contractions: bool

    def with_overrides(self, **kwargs: float | bool) -> "FilterThresholds":
        data = {
            "vol_contraction_max": self.vol_contraction_max,
            "volu_contraction_max": self.volu_contraction_max,
            "pullback_min": self.pullback_min,
            "pullback_max": self.pullback_max,
            "require_both_contractions": self.require_both_contractions,
        }
        data.update(kwargs)
        return FilterThresholds(**data)


def evaluate_filters(
    features: Dict[str, float],
    market: str,
    thresholds: FilterThresholds,
    *,
    require_volume: bool,
) -> tuple[bool, List[str]]:
    reasons: List[str] = []
    close = features.get("close")
    ma20 = features.get("ma20")
    ma50 = features.get("ma50")
    pullback = features.get("pullback_pct")
    vol_c = features.get("vol_contraction")
    volu_c = features.get("volu_contraction")
    slope = features.get("ma20_slope")
    volume_missing = bool(features.get("volume_missing"))

    if volume_missing and require_volume:
        reasons.append("volume_missing")
    if close is None or ma20 is None or ma50 is None:
        reasons.append("missing_ma")
    else:
        if not (close > ma20 and close > ma50):
            reasons.append("close_below_ma")
    if slope is None or slope <= 0:
        reasons.append("ma20_slope_nonpos")

    if pullback is None:
        reasons.append("pullback_missing")
    else:
        low = thresholds.pullback_min
        high = thresholds.pullback_max
        if high <= 1.0:
            low *= 100.0
            high *= 100.0
        if not (low <= pullback <= high):
            reasons.append("pullback_out_of_band")

    vol_c_missing = _is_missing(vol_c)
    volu_missing = _is_missing(volu_c)
    if thresholds.require_both_contractions:
        if vol_c_missing or vol_c > thresholds.vol_contraction_max:
            reasons.append("vol_contraction_fail")
        if not volume_missing and (volu_missing or volu_c > thresholds.volu_contraction_max):
            reasons.append("volu_contraction_fail")
    else:
        vol_c_ok = (not vol_c_missing) and vol_c <= thresholds.vol_contraction_max
        volu_ok = volume_missing or ((not volu_missing) and volu_c <= thresholds.volu_contraction_max)
        if not (vol_c_ok or volu_ok):
            reasons.append("vol_contraction_fail")
            if not volume_missing:
                reasons.append("volu_contraction_fail")

    return (len(reasons) == 0, reasons)


def resolve_pb1_phase(
    now: datetime,
    trading_day: bool,
    force_phase_env: str | None = None,
) -> tuple[str, str, str]:
    force_raw = (force_phase_env or "").strip().lower()
    if force_raw:
        if force_raw in {"entry", "exit", "verify"}:
            window = resolve_market_window(now, trading_day)
            return force_raw, "force", window
        logger.warning("[PB1][PHASE] invalid force phase=%s -> auto", force_raw)
    window = resolve_market_window(now, trading_day)
    if not trading_day:
        return "exit", "auto_non_trading_day", window
    entry_start = datetime.combine(now.date(), datetime.strptime(PB1_ENTRY_WINDOW_START, "%H:%M").time(), now.tzinfo)
    entry_open_end = datetime.combine(now.date(), datetime.strptime(PB1_ENTRY_OPEN_END, "%H:%M").time(), now.tzinfo)
    entry_end = datetime.combine(now.date(), datetime.strptime(PB1_ENTRY_WINDOW_END, "%H:%M").time(), now.tzinfo)
    exit_start = datetime.combine(now.date(), datetime.strptime(PB1_EXIT_WINDOW_START, "%H:%M").time(), now.tzinfo)
    exit_end = datetime.combine(now.date(), datetime.strptime(PB1_EXIT_WINDOW_END, "%H:%M").time(), now.tzinfo)
    if entry_start <= now < entry_open_end:
        return "entry", "auto_opening", window
    if entry_open_end <= now < entry_end:
        return "entry", "auto_day", window
    if exit_start <= now <= exit_end:
        return "exit", "auto_close", window
    return "exit", "auto_outside_window", window


def compute_window(now_kst: datetime) -> str:
    if now_kst.tzinfo is None:
        now_kst = now_kst.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    trading_day = now_kst.weekday() < 5
    return resolve_market_window(now_kst, trading_day)


class PB1Engine:
    STRATEGY_NAME = "pb1_pullback_close"
    UNIVERSE_STRATEGY = "best_k_meta"

    def __init__(
        self,
        *,
        universe_repo: UniverseRepo,
        orders_repo: OrdersRepo,
        fills_repo: FillsRepo,
        positions_repo: PositionsRepo,
        kis: KisAPI | None,
        window: WindowDecision,
        window_label: str,
        phase: str,
        dry_run: bool,
        env: str,
        run_id: str,
        now_kst_value: datetime | None = None,
    ) -> None:
        self.universe_repo = universe_repo
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.kis = kis
        self.window = window
        self.window_label = window_label
        self.phase = phase
        self.dry_run = dry_run
        self.env = env
        self.run_id = run_id
        self.balance_api_calls = 0
        self.balance_cache_hits = 0
        self.balance_tick_cache_hits = 0
        self.require_volume = env_bool("PB1_REQUIRE_VOLUME", PB1_REQUIRE_VOLUME)
        self.min_candles = int(PB1_MIN_CANDLES)
        providers = []
        if kis:
            providers.append(KISOHLCVProvider(kis))
        providers.append(KRXOHLCVProvider())
        self.ohlcv_provider = ChainOHLCVProvider(providers, env=env)
        self._setup_reason_counter: Counter[str] = Counter()
        self._now_kst = now_kst_value or now_kst()
        self._today = self._now_kst.date().isoformat()
        self._universe_as_of = None
        self._warned_keys: set[str] = set()
        self._balance_price_map: Dict[str, float] = {}
        self._balance_cost: float | None = None
        self._balance_snapshot: dict | None = None
        self._holdings_summary: Dict[str, Any] = {}
        self.filter_thresholds = self._resolve_filter_thresholds()
        entry_flag = parse_env_flag("PB1_ENTRY_ENABLED", default=PB1_ENTRY_ENABLED)
        self.entry_enabled = entry_flag.value
        self.entry_flag_valid = entry_flag.valid
        self.entry_flag_raw = entry_flag.raw
        self.window_internal = self._resolve_window_internal()

    def _resolve_window_internal(self) -> str:
        internal = compute_window(self._now_kst)
        if self.window_label in {"morning", "day"}:
            internal = self.window_label
        return internal

    @staticmethod
    def _float_env(name: str, default: float) -> float:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except ValueError:
            logger.warning("[PB1][ENV] invalid %s=%s fallback=%s", name, raw, default)
            return default

    @staticmethod
    def _int_env(name: str, default: int) -> int:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            return int(raw)
        except ValueError:
            logger.warning("[PB1][ENV] invalid %s=%s fallback=%s", name, raw, default)
            return default

    def _resolve_entry_cutoff(self) -> tuple[datetime, str]:
        raw = (os.getenv("ENTRY_CUTOFF_TIME") or PB1_ENTRY_WINDOW_END or "").strip()
        if not raw:
            raw = "15:15"
        try:
            cutoff_time = datetime.strptime(raw, "%H:%M").time()
        except ValueError:
            logger.warning("[PB1][ENV] invalid ENTRY_CUTOFF_TIME=%s fallback=%s", raw, PB1_ENTRY_WINDOW_END)
            cutoff_time = datetime.strptime(PB1_ENTRY_WINDOW_END, "%H:%M").time()
            raw = PB1_ENTRY_WINDOW_END
        cutoff = datetime.combine(self._now_kst.date(), cutoff_time, tzinfo=self._now_kst.tzinfo)
        return cutoff, raw

    def _warn_once(self, key: str, message: str, *args: object) -> None:
        if key in self._warned_keys:
            return
        self._warned_keys.add(key)
        logger.warning(message, *args)

    @staticmethod
    def _resolve_filter_thresholds() -> FilterThresholds:
        vol_max = PB1Engine._float_env("PB1_VOL_CONTRACTION_MAX", 1.05)
        volu_max = PB1Engine._float_env("PB1_VOLU_CONTRACTION_MAX", 1.10)
        pullback_min = PB1Engine._float_env("PB1_PULLBACK_MIN", 0.02)
        pullback_max = PB1Engine._float_env("PB1_PULLBACK_MAX", 0.18)
        require_both = env_bool("PB1_REQUIRE_BOTH_CONTRACTIONS", True)
        return FilterThresholds(
            vol_contraction_max=vol_max,
            volu_contraction_max=volu_max,
            pullback_min=pullback_min,
            pullback_max=pullback_max,
            require_both_contractions=require_both,
        )

    @staticmethod
    def _resolve_strict_thresholds() -> FilterThresholds:
        return FilterThresholds(
            vol_contraction_max=PB1Engine._float_env("PB1_VOL_CONTRACTION_MAX", 0.95),
            volu_contraction_max=PB1Engine._float_env("PB1_VOLU_CONTRACTION_MAX", 0.90),
            pullback_min=PB1Engine._float_env("PB1_PULLBACK_MIN", 0.05),
            pullback_max=PB1Engine._float_env("PB1_PULLBACK_MAX", 0.12),
            require_both_contractions=env_bool("PB1_REQUIRE_BOTH_CONTRACTIONS", True),
        )

    def _record_setup_reasons(self, reasons: Iterable[str]) -> None:
        for reason in reasons:
            if not reason:
                continue
            self._setup_reason_counter[reason] += 1

    def _log_reason_summary(self, note: str | None = None) -> None:
        if not self._setup_reason_counter:
            return
        top = self._setup_reason_counter.most_common(3)
        logger.info(
            "[PB1][SETUP-REASONS] total_bad=%s top3=%s%s",
            sum(self._setup_reason_counter.values()),
            top,
            f" note={note}" if note else "",
        )

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            if isinstance(value, str):
                value = value.replace(",", "").strip()
            fval = float(value)
            if fval != fval:  # NaN guard
                return None
            return fval
        except Exception:
            return None

    def _extract_holdings_prices(self, holdings_rows: Iterable[dict]) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for row in holdings_rows or []:
            try:
                code = str(row.get("pdno") or row.get("code") or "").zfill(6)
                px = self._to_float(row.get("prpr") or row.get("stck_prpr"))
                if code and px is not None:
                    prices[code] = px
            except Exception:
                continue
        return prices

    def _sum_cost_from_rows(self, rows: Iterable[dict]) -> float:
        total = 0.0
        for row in rows or []:
            val = self._to_float(row.get("pchs_amt"))
            if val:
                total += val
        return total

    def _extract_holdings_cost(self, holdings_rows: Iterable[dict], holdings_summary: dict | None) -> float | None:
        summary = _as_first_dict(holdings_summary)
        for key in COST_KEYS:
            cost = self._to_float(summary.get(key))
            if cost and cost > 0:
                return cost
        total = self._sum_cost_from_rows(holdings_rows)
        return total if total > 0 else None

    def _extract_available_cash(self, holdings_summary: dict | None) -> float | None:
        summary = _as_first_dict(holdings_summary)
        for key in CASH_KEYS:
            cash = self._to_float(summary.get(key))
            if cash is not None:
                return cash
        return None

    @staticmethod
    def _record_drop(
        counter: Counter[str],
        examples: Dict[str, list[str]],
        reason: str,
        code: str,
        *,
        limit: int = 3,
    ) -> None:
        if not reason:
            return
        counter[reason] += 1
        sample_list = examples.setdefault(reason, [])
        if len(sample_list) < limit:
            sample_list.append(code)

    def _fetch_holdings_snapshot(self) -> dict:
        if self._balance_snapshot is not None:
            self.balance_tick_cache_hits += 1
            logger.info("[BALANCE][CACHE] hit=True source=tick_cache")
            return self._balance_snapshot
        if not self.kis:
            return {}
        snap, source = self.kis.get_balance_cached(return_source=True)
        if source == "api":
            self.balance_api_calls += 1
        else:
            self.balance_cache_hits += 1
        logger.info("[BALANCE][CACHE] hit=%s source=%s", source != "api", source)
        self._balance_snapshot = snap
        return self._balance_snapshot

    def _client_order_key(self, code: str, mode: int, side: str, window_tag: str, stage: str) -> str:
        return f"{self._today}|{code}|sid=1|mode={mode}|{side}|{window_tag}|{stage}"

    def _log_setup(self, cf: CandidateFeature) -> None:
        prefix = "[PB1][SETUP-OK]" if cf.setup_ok else "[PB1][SETUP-BAD]"
        if not cf.setup_ok:
            self._record_setup_reasons(cf.reasons or ["unspecified_fail"])
        logger.info(
            "%s code=%s market=%s mode=%s reasons=%s features=%s",
            prefix,
            cf.code,
            cf.market,
            cf.mode,
            cf.reasons or ["n/a"],
            {k: cf.features.get(k) for k in ["close", "ma20", "ma50", "pullback_pct", "vol_contraction", "volu_contraction"]},
        )

    def _fetch_daily(self, code: str, count: int = 120) -> tuple[pd.DataFrame, Dict]:
        try:
            result = self.ohlcv_provider.get_ohlcv(code, count)
        except Exception:
            logger.exception("[PB1][DATA][FAIL] code=%s", code)
            return pd.DataFrame(), {"volume_missing": True, "source": "error", "mapped": {}}
        if not result or result.df is None or result.df.empty:
            return pd.DataFrame(), (result.meta if result else {"volume_missing": True, "source": "none"})
        df_norm = result.df.sort_values("date").tail(count)
        meta = result.meta or {}
        meta.setdefault("volume_missing", df_norm["volume"].isna().all() if "volume" in df_norm.columns else True)
        return df_norm, meta

    def _compute_candidates(self, members: Iterable[dict]) -> List[CandidateFeature]:
        candidates: List[CandidateFeature] = []
        for m in members:
            code = str(m.get("code") or "").zfill(6)
            market = m.get("market") or ""
            try:
                df, meta = self._fetch_daily(code, count=120)
                if df.empty:
                    reasons = ["data_empty"]
                    cf = CandidateFeature(
                        code=code,
                        market=market,
                        features={"reasons": reasons, "data_ok": False},
                        setup_ok=False,
                        reasons=reasons,
                        mode=1,
                        mode_reasons=["default_day_mode"],
                    )
                    candidates.append(cf)
                    continue
                if len(df) < self.min_candles:
                    reasons = ["insufficient_candles"]
                    cf = CandidateFeature(
                        code=code,
                        market=market,
                        features={"reasons": reasons, "count": len(df), "data_ok": False},
                        setup_ok=False,
                        reasons=reasons,
                        mode=1,
                        mode_reasons=["default_day_mode"],
                    )
                    candidates.append(cf)
                    continue
                try:
                    features = compute_features(df, min_candles=self.min_candles)
                except ValueError:
                    reasons = ["insufficient_candles"]
                    cf = CandidateFeature(
                        code=code,
                        market=market,
                        features={"reasons": reasons, "count": len(df), "data_ok": False},
                        setup_ok=False,
                        reasons=reasons,
                        mode=1,
                        mode_reasons=["default_day_mode"],
                    )
                    candidates.append(cf)
                    continue
                features["market"] = market
                features["volume_missing"] = bool(meta.get("volume_missing"))
                features["data_ok"] = True
                if features.get("volume_missing"):
                    features["volu_contraction"] = None
                mode, mode_reasons = choose_mode(features)
                cf = CandidateFeature(
                    code=code,
                    market=market,
                    features=features,
                    setup_ok=False,
                    reasons=[],
                    mode=mode,
                    mode_reasons=mode_reasons,
                )
                candidates.append(cf)
            except Exception:
                logger.exception("[PB1][DAILY] fetch/normalize failed code=%s", code)
                continue
        return candidates

    @staticmethod
    def _clone_candidate(cf: CandidateFeature) -> CandidateFeature:
        return CandidateFeature(
            code=cf.code,
            market=cf.market,
            features=dict(cf.features),
            setup_ok=cf.setup_ok,
            reasons=list(cf.reasons),
            mode=cf.mode,
            mode_reasons=list(cf.mode_reasons),
            client_order_key=cf.client_order_key,
            planned_qty=cf.planned_qty,
        )

    def _apply_thresholds(
        self,
        candidates: List[CandidateFeature],
        thresholds: FilterThresholds,
        *,
        log_results: bool,
    ) -> List[CandidateFeature]:
        evaluated: List[CandidateFeature] = []
        for cf in candidates:
            clone = self._clone_candidate(cf)
            data_ok = bool(clone.features.get("data_ok"))
            if not data_ok:
                if log_results:
                    self._log_setup(clone)
                evaluated.append(clone)
                continue
            ok, reasons = evaluate_filters(clone.features, clone.market, thresholds, require_volume=self.require_volume)
            if clone.features.get("volume_missing") and "volume_missing" not in reasons and self.require_volume:
                reasons.append("volume_missing")
            if ok:
                reasons = []
            elif not reasons:
                reasons = ["unspecified_fail"]
            clone.setup_ok = ok
            clone.reasons = reasons
            if log_results:
                self._log_setup(clone)
            evaluated.append(clone)
        return evaluated

    @staticmethod
    def _collect_reason_counts(candidates: Iterable[CandidateFeature]) -> Counter[str]:
        counts: Counter[str] = Counter()
        for cf in candidates:
            if cf.setup_ok:
                continue
            reasons = cf.reasons or ["unspecified_fail"]
            for reason in reasons:
                counts[reason] += 1
        return counts

    def _select_candidates_with_fallback(
        self,
        candidates: List[CandidateFeature],
    ) -> tuple[List[CandidateFeature], str, FilterThresholds, Counter[str], list[str]]:
        strict_thresholds = self._resolve_strict_thresholds()
        medium_thresholds = self.filter_thresholds.with_overrides(
            vol_contraction_max=1.00,
            volu_contraction_max=1.00,
            pullback_min=0.03,
            pullback_max=0.15,
            require_both_contractions=True,
        )
        loose_thresholds = self.filter_thresholds.with_overrides(
            vol_contraction_max=1.10,
            volu_contraction_max=1.10,
            pullback_min=0.02,
            pullback_max=0.18,
            require_both_contractions=False,
        )
        tiers = [
            ("tier1", strict_thresholds),
            ("tier2", medium_thresholds),
            ("tier3", loose_thresholds),
        ]
        selected_candidates: List[CandidateFeature] = []
        selected_tier = tiers[-1][0]
        selected_thresholds = tiers[-1][1]
        tiers_tried: list[str] = []
        all_reason_counts: Counter[str] = Counter()

        for tier_name, thresholds in tiers:
            tiers_tried.append(tier_name)
            evaluated = self._apply_thresholds(candidates, thresholds, log_results=False)
            ok_count = len([c for c in evaluated if c.setup_ok])
            reason_counts = self._collect_reason_counts(evaluated)
            all_reason_counts.update(reason_counts)
            logger.info(
                "[PB1][CANDIDATES] tier=%s ok=%s total=%s thresholds={vol_max:%.2f volu_max:%.2f pullback_min:%.3f pullback_max:%.3f require_both:%s}",
                tier_name,
                ok_count,
                len(evaluated),
                thresholds.vol_contraction_max,
                thresholds.volu_contraction_max,
                thresholds.pullback_min,
                thresholds.pullback_max,
                thresholds.require_both_contractions,
            )
            if ok_count > 0:
                selected_candidates = self._apply_thresholds(candidates, thresholds, log_results=True)
                selected_tier = tier_name
                selected_thresholds = thresholds
                break
            selected_candidates = evaluated
            selected_tier = tier_name
            selected_thresholds = thresholds

        if selected_candidates and all(c.setup_ok is False for c in selected_candidates):
            selected_candidates = self._apply_thresholds(candidates, selected_thresholds, log_results=True)
        return selected_candidates, selected_tier, selected_thresholds, all_reason_counts, tiers_tried

    def _apply_score_fallback(self, candidates: List[CandidateFeature]) -> int:
        scored: list[CandidateFeature] = []
        for cf in candidates:
            if not cf.features.get("data_ok"):
                continue
            try:
                score = float(score_setup(cf.features, cf.market))
            except Exception:
                score = 0.0
            cf.features["score"] = score
            scored.append(cf)

        if not scored:
            return 0

        scored.sort(key=lambda c: float(c.features.get("score") or 0.0), reverse=True)
        max_n = max(1, int(PB1_MAX_POSITIONS))
        selected = scored[:max_n]
        selected_codes = {c.code for c in selected}
        for cf in candidates:
            if cf.code in selected_codes:
                cf.setup_ok = True
                cf.features["score_fallback"] = True
                cf.reasons = list(cf.reasons or []) + ["score_fallback"]
        logger.info(
            "[PB1][CANDIDATES][FALLBACK] mode=score_based selected=%s total=%s",
            len(selected_codes),
            len(candidates),
        )
        return len(selected_codes)

    def _size_positions(self, candidates: List[CandidateFeature]) -> List[CandidateFeature]:
        ok_list = [c for c in candidates if c.setup_ok]
        if not ok_list:
            return candidates

        # 1) 점수 계산 + ATR/유동성 컷 + 점수 컷
        filtered: List[CandidateFeature] = []
        for cf in ok_list:
            try:
                score = float(score_setup(cf.features, cf.market))
            except Exception:
                score = 0.0
            cf.features["score"] = score

            atr_pct = cf.features.get("atr_pct")
            value20 = cf.features.get("value20")
            score_fallback = bool(cf.features.get("score_fallback"))

            if score < float(PB1_MIN_SCORE) and not score_fallback:
                cf.setup_ok = False
                cf.reasons.append("score_below_cut")
                continue
            if atr_pct is None or (isinstance(atr_pct, float) and atr_pct != atr_pct):
                cf.setup_ok = False
                cf.reasons.append("atr_pct_missing")
                continue
            if float(atr_pct) > float(PB1_MAX_ATR_PCT):
                cf.setup_ok = False
                cf.reasons.append("atr_pct_too_high")
                continue
            if value20 is None or (isinstance(value20, float) and value20 != value20):
                cf.setup_ok = False
                cf.reasons.append("value20_missing")
                continue
            if float(value20) < float(PB1_MIN_VALUE20):
                cf.setup_ok = False
                cf.reasons.append("liquidity_too_low")
                continue

            filtered.append(cf)

        if not filtered:
            return candidates

        # 2) 점수 내림차순 Top N 선택
        filtered.sort(key=lambda c: float(c.features.get("score") or 0.0), reverse=True)
        max_n = max(1, int(PB1_MAX_POSITIONS))
        selected = filtered[:max_n]
        selected_codes = {c.code for c in selected}

        # 선택되지 않은 나머지는 매수 제외 처리
        for cf in ok_list:
            if cf.code not in selected_codes and cf.setup_ok:
                cf.setup_ok = False
                cf.reasons.append("not_in_topN")

        # 3) 사이징: 리스크 패리티(ATR) 또는 균등
        cap_total = float(DAILY_CAPITAL) * float(CAP_CAP)

        if PB1_USE_RISK_PARITY:
            inv: List[float] = []
            for cf in selected:
                atr = float(cf.features.get("atr14") or 0.0)
                inv.append(1.0 / max(atr, 1e-6))
            inv_sum = sum(inv) if sum(inv) > 0 else 1.0
            weights = [x / inv_sum for x in inv]
        else:
            weights = [1.0 / len(selected)] * len(selected)

        for cf, w in zip(selected, weights):
            close_px = float(cf.features.get("close") or 0.0)
            if close_px <= 0:
                cf.setup_ok = False
                cf.reasons.append("close_zero")
                continue

            capital = cap_total * float(w)
            qty = int(capital // close_px)
            cf.planned_qty = max(qty, 0)
            cf.client_order_key = self._client_order_key(cf.code, cf.mode, "BUY", "close", "PB1")

            if cf.planned_qty <= 0:
                cf.setup_ok = False
                cf.reasons.append("planned_qty_zero")

            logger.info(
                "[PB1][RANK] code=%s score=%.1f w=%.3f cap=%.0f qty=%s atr_pct=%.2f value20=%s",
                cf.code,
                float(cf.features.get("score") or 0.0),
                float(w),
                float(capital),
                cf.planned_qty,
                float(cf.features.get("atr_pct") or 0.0),
                cf.features.get("value20"),
            )

        return candidates

    def _mark_price(self, code: str) -> float | None:
        if self.kis:
            try:
                diag_mode = self.dry_run or self.phase == "verify" or (self.window and self.window.name == "diagnostic")
                quote = self.kis.get_price_quote(code, diag_mode=diag_mode)
                if not isinstance(quote, dict):
                    self._warn_once(f"quote_non_dict:{code}", "[PB1][PRICE][WARN] code=%s non-dict quote", code)
                    return None
                price = quote.get("last")
                if price is None:
                    price = quote.get("stck_prpr") or quote.get("prpr")
                    if price is None:
                        self._warn_once(
                            f"quote_missing_last:{code}", "[PB1][PRICE][WARN] code=%s missing_last keys=%s", code, list(quote.keys())
                        )
                        return None
                try:
                    price_val = float(price)
                except Exception:
                    self._warn_once(f"quote_invalid_price:{code}", "[PB1][PRICE][WARN] code=%s invalid price=%s", code, price)
                    return None
                if quote.get("ask") is None or quote.get("bid") is None:
                    self._warn_once(
                        f"quote_missing_book:{code}",
                        "[PB1][PRICE][WARN] code=%s ask=%s bid=%s",
                        code,
                        quote.get("ask"),
                        quote.get("bid"),
                    )
                return price_val
            except Exception:
                self._warn_once(f"quote_fail:{code}", "[PB1][PRICE][FAIL] code=%s", code)
        return None

    def _fetch_marks(self, codes: Iterable[str], fallback: Dict[str, float]) -> Dict[str, float]:
        marks: Dict[str, float] = {}
        for code in codes:
            px = self._mark_price(code)
            if px is None:
                px = self._balance_price_map.get(code) or fallback.get(code)
            if px is not None:
                marks[code] = px
        return marks

    def _should_block_order(self, client_order_key: str) -> bool:
        if not client_order_key:
            return True
        return self.orders_repo.has_client_order_key(self.env, client_order_key)

    def _place_entry(self, cf: CandidateFeature) -> None:
        order_id = self.orders_repo.create_intent_idempotent(
            env=self.env,
            run_id=self.run_id,
            strategy=self.STRATEGY_NAME,
            sid=1,
            mode=cf.mode,
            code=cf.code,
            market=cf.market,
            side="BUY",
            ord_type="MARKET",
            qty=cf.planned_qty,
            limit_price=cf.features.get("close"),
            stage="PB1-CLOSE",
            client_order_key=cf.client_order_key or "",
            request_json={"features": cf.features, "reasons": cf.reasons},
        )
        if self.dry_run:
            logger.info("[PB1][ENTRY-DRY] code=%s qty=%s key=%s order_id=%s", cf.code, cf.planned_qty, cf.client_order_key, order_id)
            return
        if not self.kis:
            logger.warning("[PB1][ENTRY][SKIP] KIS missing code=%s", cf.code)
            return
        resp = None
        kis_odno = None
        try:
            resp = self.kis.buy_stock_market(cf.code, cf.planned_qty)
            kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        except Exception:
            logger.exception("[PB1][ENTRY][FAIL] code=%s", cf.code)
        self.orders_repo.mark_submitted(self.env, cf.client_order_key or "", kis_odno, resp if isinstance(resp, dict) else {"resp": resp})
        if resp and isinstance(resp, dict) and resp.get("rt_cd") == "0":
            self.orders_repo.mark_acked(self.env, kis_odno, resp)
            filled_at = now_kst()
            self.fills_repo.upsert_fill(
                env=self.env,
                run_id=self.run_id,
                order_id=order_id,
                kis_odno=kis_odno,
                trade_id=None,
                code=cf.code,
                market=cf.market,
                side="BUY",
                qty=cf.planned_qty,
                price=cf.features.get("close") or 0.0,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
                raw_json=resp,
            )
            self.positions_repo.apply_fill(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=cf.mode,
                code=cf.code,
                market=cf.market,
                side="BUY",
                qty=cf.planned_qty,
                price=cf.features.get("close") or 0.0,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
            )
        else:
            self.orders_repo.mark_error(self.env, cf.client_order_key or "", resp if isinstance(resp, dict) else {"resp": resp})

    def _plan_exit_event(self, pos: Dict, features: Dict[str, float], window_tag: str) -> None:
        avg = pos.get("avg_buy_price")
        if not avg:
            return
        code = pos.get("code")
        market = pos.get("market")
        mode = pos.get("mode")
        if pos.get("sid") != 1:
            return
        qty = pos.get("qty") or 0
        if qty <= 0:
            return
        mark = self._mark_price(code)
        if mark is None:
            mark = self._balance_price_map.get(code)
        if mark is None:
            mark = features.get("close") or avg
        ret_pct = ((mark - avg) / avg) * 100 if avg else 0.0
        client_key = self._client_order_key(code, mode, "SELL", window_tag, "exit")
        if self._should_block_order(client_key):
            logger.info("[PB1][EXIT-SKIP] code=%s mode=%s reason=dup key=%s", code, mode, client_key)
            return

        if mode == 1:
            atr_pct = ((features.get("atr14") or 0.0) / avg) * 100
            r_pct = max(PB1_R_FLOOR_PCT, atr_pct)
            take_profit = PB1_DAY_TP_R * r_pct
            stop_loss = PB1_DAY_SL_R * r_pct
            if window_tag != "morning":
                return
            stage = "DAY-EXIT"
            reasons: list[str] = []
            if ret_pct >= take_profit:
                reasons.append("take_profit")
            if ret_pct <= -stop_loss:
                reasons.append("stop_loss")
            if not reasons:
                reasons.append("time_exit")
        else:
            hard_stop = KOSDAQ_HARD_STOP_PCT if market == "KOSDAQ" else KOSPI_HARD_STOP_PCT
            if ret_pct <= -hard_stop:
                stage = "HARD-STOP"
                if window_tag not in {"morning", "close"}:
                    return
            else:
                if window_tag != "close":
                    return
                close_px = features.get("close")
                ma20 = features.get("ma20")
                holding_days = pos.get("holding_days") or 0
                if holding_days >= PB1_TIME_STOP_DAYS:
                    stage = "TIME-STOP"
                elif close_px is not None and ma20 is not None and close_px < ma20:
                    stage = "MA20-TRAIL"
                else:
                    return
            reasons = ["pb1_exit"]

        order_id = self.orders_repo.create_intent_idempotent(
            env=self.env,
            run_id=self.run_id,
            strategy=self.STRATEGY_NAME,
            sid=1,
            mode=mode,
            code=code,
            market=market,
            side="SELL",
            ord_type="MARKET",
            qty=qty,
            limit_price=mark,
            stage=stage,
            client_order_key=client_key,
            request_json={"reasons": reasons, "ret_pct": ret_pct},
        )
        if self.dry_run:
            logger.info("[PB1][EXIT-DRY] code=%s qty=%s key=%s order_id=%s", code, qty, client_key, order_id)
            return
        if not self.kis:
            logger.warning("[PB1][EXIT][SKIP] kis missing code=%s", code)
            return
        resp = None
        kis_odno = None
        try:
            resp = self.kis.sell_stock_market(code, qty)
            kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        except Exception:
            logger.exception("[PB1][EXIT][FAIL] code=%s", code)
        self.orders_repo.mark_submitted(self.env, client_key, kis_odno, resp if isinstance(resp, dict) else {"resp": resp})
        if resp and isinstance(resp, dict) and resp.get("rt_cd") == "0":
            self.orders_repo.mark_acked(self.env, kis_odno, resp)
            filled_at = now_kst()
            self.fills_repo.upsert_fill(
                env=self.env,
                run_id=self.run_id,
                order_id=order_id,
                kis_odno=kis_odno,
                trade_id=None,
                code=code,
                market=market,
                side="SELL",
                qty=qty,
                price=mark,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
                raw_json=resp,
            )
            self.positions_repo.apply_fill(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                market=market,
                side="SELL",
                qty=qty,
                price=mark,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
            )
        else:
            self.orders_repo.mark_error(self.env, client_key, resp if isinstance(resp, dict) else {"resp": resp})

    def _positions_with_meta(self, positions: Iterable[Dict]) -> List[Dict]:
        enriched: List[Dict] = []
        for state in positions:
            if state.get("sid") != 1:
                continue
            enriched.append(
                {
                    "code": state.get("code"),
                    "sid": state.get("sid"),
                    "mode": state.get("mode"),
                    "qty": state.get("qty") or 0,
                    "avg_buy_price": state.get("avg_buy_price"),
                    "market": state.get("market"),
                    "holding_days": state.get("holding_days") or 0,
                    "first_buy_ts": state.get("first_buy_ts"),
                    "total_cost": state.get("total_cost") or 0.0,
                    "realized_pnl": state.get("realized_pnl") or 0.0,
                }
            )
        return enriched

    def _load_universe(self) -> list[dict]:
        today = self._now_kst.date().isoformat()
        members = self.universe_repo.get_universe_members(self.env, self.UNIVERSE_STRATEGY, today)
        self._universe_as_of = today if members else None
        if not members:
            members = self.universe_repo.get_latest_universe_members(self.env, self.UNIVERSE_STRATEGY)
            if members:
                self._universe_as_of = members[0].get("as_of_date")
        return members

    def _pnl_snapshot(self, positions: List[Dict]) -> Dict[str, float]:
        fallback: Dict[str, float] = {p["code"]: p.get("avg_buy_price") or 0.0 for p in positions}
        marks = self._fetch_marks([p["code"] for p in positions], fallback)
        totals: Dict[str, float] = {"market_value": 0.0, "cost": 0.0, "unrealized": 0.0, "realized": 0.0}
        for pos in positions:
            qty = pos.get("qty") or 0
            mark = marks.get(pos["code"]) or self._balance_price_map.get(pos["code"]) or pos.get("avg_buy_price") or 0.0
            market_value = float(mark) * qty
            cost = float(pos.get("total_cost") or 0.0)
            totals["market_value"] += market_value
            totals["cost"] += cost
            totals["realized"] += float(pos.get("realized_pnl") or 0.0)
            totals["unrealized"] += market_value - cost

        summary_mv = None
        for key in EVAL_KEYS:
            summary_mv = self._to_float(self._holdings_summary.get(key))
            if summary_mv is not None:
                break
        summary_cash = None
        for key in CASH_KEYS:
            summary_cash = self._to_float(self._holdings_summary.get(key))
            if summary_cash is not None:
                break
        if not positions and summary_mv is not None:
            totals["market_value"] = summary_mv
            totals["unrealized"] = 0.0
        if summary_cash is not None:
            totals["cash"] = summary_cash

        cost_source = "positions"
        summary_cost = self._extract_holdings_cost([], self._holdings_summary)
        cost_base = self._balance_cost if self._balance_cost is not None else totals["cost"] or summary_cost or summary_mv
        if self._balance_cost is not None:
            cost_source = "kis_balance"
            totals["cost"] = self._balance_cost
        elif summary_cost:
            cost_source = "balance_summary"
            totals["cost"] = summary_cost

        balance_unrealized = None
        for row in self._balance_snapshot.get("output1", []) if self._balance_snapshot else []:
            for key in UNREALIZED_KEYS:
                val = self._to_float(row.get(key))
                if val is not None:
                    balance_unrealized = (balance_unrealized or 0.0) + val
        summary_unrealized = None
        for key in UNREALIZED_KEYS:
            summary_unrealized = self._to_float(self._holdings_summary.get(key))
            if summary_unrealized is not None:
                break
        unrealized_source = "positions"
        if balance_unrealized is not None:
            totals["unrealized"] = balance_unrealized
            unrealized_source = "kis_balance_rows"
        elif summary_unrealized is not None:
            totals["unrealized"] = summary_unrealized
            unrealized_source = "kis_balance_summary"

        return_pct_source = "computed"
        balance_return_pct = None
        for key in RETURN_PCT_KEYS:
            balance_return_pct = self._to_float(self._holdings_summary.get(key))
            if balance_return_pct is not None:
                return_pct_source = f"kis_balance:{key}"
                break

        portfolio_return_pct = balance_return_pct
        if portfolio_return_pct is None:
            if cost_base and cost_base > 0:
                portfolio_return_pct = (totals["market_value"] - cost_base + totals["realized"]) / cost_base * 100
            else:
                self._warn_once("pnl_zero_cost", "[PNL][SNAPSHOT][WARN] zero_or_missing_cost -> return_pct=N/A")

        invested_return_pct = None
        invested_pnl = self._to_float(self._holdings_summary.get("evlu_pfls_smtl_amt"))
        invested_cost = self._to_float(self._holdings_summary.get("pchs_amt_smtl_amt"))
        if invested_pnl is not None and invested_cost and invested_cost > 0:
            invested_return_pct = (invested_pnl / invested_cost) * 100

        total_asset_return_pct = self._to_float(self._holdings_summary.get("asst_icdc_erng_rt"))

        realized_source = "ledger" if totals["realized"] != 0.0 else "none"

        logger.info(
            "[PNL][SNAPSHOT] universe_as_of=%s market_value=%.2f cost=%.2f cost_source=%s unrealized=%.2f unrealized_source=%s realized=%.2f realized_source=%s return_pct=%s return_pct_source=%s invested_return_pct=%s total_asset_return_pct=%s",
            self._universe_as_of or "none",
            totals["market_value"],
            totals["cost"],
            cost_source,
            totals["unrealized"],
            unrealized_source,
            totals["realized"],
            realized_source,
            f"{portfolio_return_pct:.2f}" if portfolio_return_pct is not None else "N/A",
            return_pct_source,
            f"{invested_return_pct:.2f}" if invested_return_pct is not None else "N/A",
            f"{total_asset_return_pct:.2f}" if total_asset_return_pct is not None else "N/A",
        )
        totals["return_pct"] = portfolio_return_pct if portfolio_return_pct is not None else 0.0
        return totals

    def run(self) -> RunResult:
        self._warned_keys.clear()
        self._setup_reason_counter.clear()
        final_status = "OK"
        final_notes: str | None = None
        entry_allowed = self.entry_enabled
        entry_cutoff_dt, entry_cutoff_raw = self._resolve_entry_cutoff()
        max_positions = int(PB1_MAX_POSITIONS)
        target_new_positions = self._int_env("PB1_TARGET_NEW_POSITIONS", max_positions)
        min_order_krw = self._float_env("MIN_ORDER_KRW", 0.0)
        entry_capital_krw = float(DAILY_CAPITAL) * float(CAP_CAP)
        skip_entry_scan = False
        if not entry_allowed:
            logger.warning("[PB1][ENTRY_DISABLED] PB1_ENTRY_ENABLED=%s raw=%s valid=%s -> skip new entries", entry_allowed, self.entry_flag_raw, self.entry_flag_valid)
        logger.info(
            "[PB1][RUN] window=%s window_internal=%s phase=%s dry_run=%s env=%s",
            self.window_label,
            self.window_internal,
            self.phase,
            self.dry_run,
            self.env,
        )

        if self.phase in {"prep", "entry", "trade"} and self._now_kst > entry_cutoff_dt:
            skip_entry_scan = True
            entry_allowed = False
            logger.info(
                "[PB1][SKIP_ENTRY] reason=entry_cutoff now=%s cutoff=%s",
                self._now_kst.isoformat(),
                entry_cutoff_dt.isoformat(),
            )
            if self.phase in {"prep", "entry"}:
                return RunResult(
                    status="SKIPPED",
                    notes="entry_cutoff",
                    balance_api_calls=self.balance_api_calls,
                    balance_cache_hits=self.balance_cache_hits,
                    balance_tick_cache_hits=self.balance_tick_cache_hits,
                )

        members = self._load_universe()
        if not members:
            note = "universe_empty"
            logger.warning("[PB1][UNIVERSE][EMPTY] env=%s strategy=%s", self.env, self.UNIVERSE_STRATEGY)
            self._log_reason_summary("universe_empty")
            return RunResult(
                status="SKIPPED",
                notes=note,
                balance_api_calls=self.balance_api_calls,
                balance_cache_hits=self.balance_cache_hits,
                balance_tick_cache_hits=self.balance_tick_cache_hits,
            )

        positions = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
        holdings_snapshot = self._fetch_holdings_snapshot()
        holdings_rows = holdings_snapshot.get("output1") or []
        holdings_summary_raw = holdings_snapshot.get("output2")
        holdings_summary = _as_first_dict(holdings_summary_raw)
        self._holdings_summary = holdings_summary
        self._balance_price_map = self._extract_holdings_prices(holdings_rows)
        self._balance_cost = self._extract_holdings_cost(holdings_rows, holdings_summary)
        available_cash_krw = self._extract_available_cash(holdings_summary)
        logger.info(
            "[PB1][RUN-START] PB1_MAX_POSITIONS=%s PB1_TARGET_NEW_POSITIONS=%s PB1_ENTRY_CAPITAL_KRW=%.0f MIN_ORDER_KRW=%.0f ENTRY_CUTOFF_TIME=%s EXISTING_POSITIONS_COUNT=%s AVAILABLE_CASH_KRW=%s",
            max_positions,
            target_new_positions,
            entry_capital_krw,
            min_order_krw,
            entry_cutoff_raw,
            len(positions),
            f"{available_cash_krw:.0f}" if available_cash_krw is not None else "N/A",
        )
        if self.phase in {"verify", "exit"}:
            holdings = list(holdings_rows or [])
            if not holdings and self.kis:
                logger.info("[PB1][HOLDINGS] empty_balance_snapshot -> skip extra fetch")
            if not positions and holdings:
                bootstrapped = self.positions_repo.bootstrap_from_kis_holdings(
                    env=self.env,
                    strategy=self.STRATEGY_NAME,
                    sid=1,
                    mode=1,
                    holdings=holdings,
                )
                logger.info("[PB1][BOOTSTRAP] positions_inserted=%s", bootstrapped)
                positions = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
            open_orders = self.orders_repo.get_open_orders(self.env)
            if open_orders:
                logger.info("[PB1][ORDERS][OPEN] count=%s", len(open_orders))
            self._pnl_snapshot(self._positions_with_meta(positions))
            if self.phase == "verify":
                final_status = "OK"
                final_notes = "verify_only"
                self._log_reason_summary(final_notes)
                return RunResult(
                    status=final_status,
                    notes=final_notes,
                    balance_api_calls=self.balance_api_calls,
                    balance_cache_hits=self.balance_cache_hits,
                    balance_tick_cache_hits=self.balance_tick_cache_hits,
                )

        code_market = {m.get("code"): m.get("market") for m in members}
        candidates: List[CandidateFeature] = []
        marks_fallback: Dict[str, float] = {}
        selected_tier = "tier1"
        selected_thresholds = self.filter_thresholds
        all_reason_counts: Counter[str] = Counter()
        tiers_tried: list[str] = []
        setup_ok_codes: list[str] = []
        drop_reason_counter: Counter[str] = Counter()
        drop_examples: Dict[str, list[str]] = {}
        after_risk_check_count = 0
        after_buyable_check_count = 0
        after_dedup_count = 0
        if self.phase in {"prep", "entry", "trade"} and not skip_entry_scan:
            candidates = self._compute_candidates(members)
            (
                candidates,
                selected_tier,
                selected_thresholds,
                all_reason_counts,
                tiers_tried,
            ) = self._select_candidates_with_fallback(candidates)
            if not any(c.setup_ok for c in candidates):
                self._apply_score_fallback(candidates)
            setup_ok_codes = [c.code for c in candidates if c.setup_ok]
            candidates = self._size_positions(candidates)
            ok_after_risk = [c for c in candidates if c.setup_ok]
            after_risk_check_count = len(ok_after_risk)
            dropped_after_risk = {c.code for c in candidates if c.code in setup_ok_codes and not c.setup_ok}
            for cf in candidates:
                if cf.code in dropped_after_risk:
                    for reason in cf.reasons or ["unspecified_fail"]:
                        self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)

            slots_remaining = max(0, max_positions - len(positions))
            new_position_limit = max(0, min(target_new_positions, slots_remaining))
            buyable_candidates: list[CandidateFeature] = []
            for cf in ok_after_risk:
                order_value = float(cf.features.get("close") or 0.0) * float(cf.planned_qty or 0)
                reasons: list[str] = []
                if new_position_limit <= 0:
                    reasons.append("max_positions")
                if target_new_positions <= 0:
                    reasons.append("target_new_positions_zero")
                if entry_capital_krw <= 0:
                    reasons.append("entry_capital_zero")
                if available_cash_krw is not None and available_cash_krw <= 0:
                    reasons.append("available_cash_zero")
                if min_order_krw > 0 and order_value < min_order_krw:
                    reasons.append("min_order_krw")
                if order_value <= 0:
                    reasons.append("order_value_zero")
                if available_cash_krw is not None and order_value > available_cash_krw:
                    reasons.append("insufficient_cash")
                if not reasons and len(buyable_candidates) >= new_position_limit:
                    reasons.append("target_new_positions_limit")
                if reasons:
                    for reason in reasons:
                        self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)
                    continue
                buyable_candidates.append(cf)

            after_buyable_check_count = len(buyable_candidates)
            dedup_candidates: list[CandidateFeature] = []
            for cf in buyable_candidates:
                if self._should_block_order(cf.client_order_key or ""):
                    self._record_drop(drop_reason_counter, drop_examples, "duplicate_order", cf.code)
                    continue
                dedup_candidates.append(cf)
            after_dedup_count = len(dedup_candidates)

            ok_count = len([c for c in candidates if c.setup_ok])
            logger.info(
                "[PB1][CANDIDATES][SUMMARY] universe=%s scanned=%s selected_tier=%s ok=%s total=%s thresholds={vol_max:%.2f volu_max:%.2f pullback_min:%.3f pullback_max:%.3f require_both:%s}",
                len(members),
                len(candidates),
                selected_tier,
                ok_count,
                len(candidates),
                selected_thresholds.vol_contraction_max,
                selected_thresholds.volu_contraction_max,
                selected_thresholds.pullback_min,
                selected_thresholds.pullback_max,
                selected_thresholds.require_both_contractions,
            )
            logger.info(
                "[PB1][CANDIDATES][SNAPSHOT] setup_ok_count=%s setup_ok_sample=%s after_risk_check_count=%s after_buyable_check_count=%s after_dedup_count=%s drop_reasons_topN=%s drop_examples=%s",
                len(setup_ok_codes),
                setup_ok_codes[:3],
                after_risk_check_count,
                after_buyable_check_count,
                after_dedup_count,
                drop_reason_counter.most_common(3),
                {k: v for k, v in drop_examples.items() if v},
            )
            if not candidates or ok_count == 0:
                top_reasons = all_reason_counts.most_common(3)
                final_status = "NO_TRADE"
                final_notes = f"no_candidates:{top_reasons or 'none'}"
                logger.info(
                    "[PB1][NO_TRADE] reason=no_candidates tiers_tried=%s tier=%s total=%s top_reasons=%s",
                    tiers_tried or ["none"],
                    selected_tier,
                    len(candidates),
                    top_reasons or "none",
                )
                if self.phase in {"prep", "entry"}:
                    self._log_reason_summary(final_notes)
                    return RunResult(
                        status=final_status,
                        notes=final_notes,
                        balance_api_calls=self.balance_api_calls,
                        balance_cache_hits=self.balance_cache_hits,
                        balance_tick_cache_hits=self.balance_tick_cache_hits,
                    )
            if self.phase in {"entry", "trade"}:
                if not entry_allowed:
                    logger.info("[PB1][ENTRY][SKIP] entry_allowed=False")
                orderable_candidates = dedup_candidates if entry_allowed else []
                if ok_count > 0 and not orderable_candidates:
                    no_orders_reasons: list[str] = []
                    if not entry_allowed:
                        no_orders_reasons.append("entry_disabled")
                    if skip_entry_scan:
                        no_orders_reasons.append("entry_cutoff")
                    if max_positions - len(positions) <= 0:
                        no_orders_reasons.append("max_positions")
                    if target_new_positions <= 0:
                        no_orders_reasons.append("target_new_positions_zero")
                    if entry_capital_krw <= 0:
                        no_orders_reasons.append("entry_capital_zero")
                    if available_cash_krw is not None and available_cash_krw <= 0:
                        no_orders_reasons.append("available_cash_zero")
                    if min_order_krw > 0 and after_buyable_check_count == 0:
                        no_orders_reasons.append("min_order_krw")
                    if any(reason == "duplicate_order" for reason, _ in drop_reason_counter.most_common()):
                        no_orders_reasons.append("duplicate_order")
                    final_status = "NO_TRADE"
                    final_notes = f"no_orders:{no_orders_reasons or 'none'}"
                    logger.info(
                        "[PB1][NO_TRADE] reason=no_orders no_orders_reason=%s",
                        no_orders_reasons or ["none"],
                    )
                    if self.phase in {"prep", "entry"}:
                        self._log_reason_summary(final_notes)
                        return RunResult(
                            status=final_status,
                            notes=final_notes,
                            balance_api_calls=self.balance_api_calls,
                            balance_cache_hits=self.balance_cache_hits,
                            balance_tick_cache_hits=self.balance_tick_cache_hits,
                        )
                for cf in orderable_candidates:
                    self._place_entry(cf)
        if self.phase in {"exit", "verify", "trade"}:
            pos_list = self._positions_with_meta(positions)
            for pos in pos_list:
                df, _ = self._fetch_daily(pos["code"], count=120)
                if df.empty:
                    continue
                try:
                    features = compute_features(df, min_candles=self.min_candles)
                except ValueError:
                    continue
                features["market"] = pos.get("market") or code_market.get(pos["code"], "")
                marks_fallback[pos["code"]] = features.get("close") or pos.get("avg_buy_price") or 0.0
                self._plan_exit_event(pos, features, "morning" if self.window_internal == "morning" else "close")
        self._pnl_snapshot(self._positions_with_meta(self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)))
        final_notes = final_notes or self._universe_as_of or "ok"
        self._log_reason_summary(final_notes)
        return RunResult(
            status=final_status,
            notes=final_notes,
            balance_api_calls=self.balance_api_calls,
            balance_cache_hits=self.balance_cache_hits,
            balance_tick_cache_hits=self.balance_tick_cache_hits,
        )
