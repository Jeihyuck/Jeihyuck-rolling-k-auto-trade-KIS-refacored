"""Trade API package."""

from __future__ import annotations

import builtins
import functools
import logging
import os
from typing import Any

_logger = logging.getLogger(__name__)


class _MissingEngineRunnerSummary:
    """Last-resort empty summary for PB1 session finalization."""

    _run_summary_payload: dict[str, Any] = {}
    _debug_summary: dict[str, Any] = {}


def _ensure_engine_runner_fallback() -> None:
    if not hasattr(builtins, "engine_runner"):
        builtins.engine_runner = _MissingEngineRunnerSummary()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _normalize_ratio(value: Any, default: float = 0.0) -> float:
    raw = _safe_float(value, default)
    return raw / 100.0 if raw > 1.0 else raw


def _candidate_code(cf: Any) -> str:
    return str(getattr(cf, "code", "") or "").zfill(6)


def _candidate_features(cf: Any) -> dict[str, Any]:
    features = getattr(cf, "features", None)
    if isinstance(features, dict):
        return features
    features = {}
    try:
        setattr(cf, "features", features)
    except Exception:
        pass
    return features


def _is_kr_code(code: str) -> bool:
    return len(str(code or "")) == 6 and str(code).isdigit()


def _collect_buyable_backfill_blocked_codes(engine: Any) -> set[str]:
    blocked: set[str] = set()
    for attr in ("_existing_holding_codes", "_open_buy_codes", "_today_buy_codes", "_cooldown_codes"):
        for code in getattr(engine, attr, []) or []:
            if str(code or "").strip():
                blocked.add(str(code).zfill(6))
    gate_context = getattr(engine, "_buyable_gate_context", {}) or {}
    if isinstance(gate_context, dict):
        for code, snapshot in gate_context.items():
            if not isinstance(snapshot, dict):
                continue
            code_key = str(code or "").zfill(6)
            if not code_key:
                continue
            if (
                _safe_int(snapshot.get("kis_holding_qty")) > 0
                or _safe_int(snapshot.get("holding_qty")) > 0
                or bool(snapshot.get("today_buy_exists"))
                or bool(snapshot.get("today_fill_exists"))
                or bool(snapshot.get("today_sell_exists"))
                or bool(snapshot.get("open_order_exists"))
                or bool(snapshot.get("cooldown_active"))
                or bool(snapshot.get("duplicate_intent_exists"))
                or bool(snapshot.get("blocking_duplicate_exists"))
            ):
                blocked.add(code_key)
    return blocked


def _buyable_backfill_block_reason(engine: Any, cf: Any, *, blocked_codes: set[str]) -> str | None:
    code = _candidate_code(cf)
    if not code:
        return "missing_code"
    if code in blocked_codes:
        return "blocked_existing_holding_or_duplicate"
    features = _candidate_features(cf)
    if bool(features.get("existing_holding")) or bool(features.get("open_order_exists")):
        return "blocked_existing_holding_or_duplicate"
    if bool(features.get("today_buy_exists")) or bool(features.get("today_fill_exists")):
        return "blocked_today_buy"
    if bool(features.get("today_sell_exists")):
        return "blocked_today_sell"
    if bool(features.get("cooldown_active")):
        return "blocked_cooldown"
    gate_snapshot = (getattr(engine, "_buyable_gate_context", {}) or {}).get(code, {})
    if isinstance(gate_snapshot, dict):
        if _safe_int(gate_snapshot.get("kis_holding_qty")) > 0 or _safe_int(gate_snapshot.get("holding_qty")) > 0:
            return "blocked_existing_holding_or_duplicate"
        for key, reason in (
            ("open_order_exists", "blocked_open_order"),
            ("today_buy_exists", "blocked_today_buy"),
            ("today_fill_exists", "blocked_today_buy"),
            ("today_sell_exists", "blocked_today_sell"),
            ("cooldown_active", "blocked_cooldown"),
            ("duplicate_intent_exists", "blocked_duplicate"),
            ("blocking_duplicate_exists", "blocked_duplicate"),
        ):
            if bool(gate_snapshot.get(key)):
                return reason
    return None


def _candidate_safe_for_buyable_backfill(cf: Any) -> tuple[bool, str]:
    code = _candidate_code(cf)
    if not _is_kr_code(code):
        return False, "not_kr_code"
    features = _candidate_features(cf)
    if features.get("data_ok") is False:
        return False, "data_not_ok"

    close = _safe_float(features.get("order_price") or features.get("entry_price") or features.get("close") or features.get("last_close"))
    if close <= 0:
        return False, "price_missing"
    ma20 = _safe_float(features.get("ma20"))
    ma50 = _safe_float(features.get("ma50"))
    ma150 = _safe_float(features.get("ma150"))
    if ma20 <= 0 or ma50 <= 0 or ma150 <= 0:
        return False, "ma_missing"

    atr_ratio = _normalize_ratio(features.get("atr_pct") if features.get("atr_pct") is not None else features.get("atr_percent"))
    max_atr = _normalize_ratio(os.getenv("PB1_BUYABLE_BACKFILL_MAX_ATR_PCT", os.getenv("PB1_MAX_ATR_PCT", "10")), 0.10)
    if atr_ratio <= 0 or atr_ratio > max_atr:
        return False, "atr_pct_out_of_range"

    setup_like_ok = bool(
        getattr(cf, "setup_ok", False)
        or features.get("setup_loose_ok")
        or features.get("setup_style_ok")
        or features.get("relax_ok")
        or features.get("minervini_buyable")
        or features.get("minervini_bridge_candidate")
        or features.get("score_fallback")
    )
    score = _safe_float(features.get("score_final") or features.get("final_score") or features.get("score"))
    min_score = _env_float("PB1_BUYABLE_BACKFILL_MIN_SCORE", 30.0)
    style = str(features.get("entry_style_selected") or features.get("entry_component") or features.get("entry_signal") or "").strip()
    if not setup_like_ok:
        if score < min_score:
            return False, "score_below_backfill_floor"
        if not style:
            return False, "entry_style_missing"
        if close < ma20 * _env_float("PB1_BUYABLE_BACKFILL_MIN_CLOSE_MA20_RATIO", 0.92):
            return False, "close_too_far_below_ma20"
        if close < ma50 * _env_float("PB1_BUYABLE_BACKFILL_MIN_CLOSE_MA50_RATIO", 0.85):
            return False, "close_too_far_below_ma50"
    return True, "ok"


def _ensure_backfill_entry_plan(engine: Any, cf: Any, *, price: float, qty: int) -> bool:
    features = _candidate_features(cf)
    stop_price = _safe_float(features.get("stop_price") or features.get("initial_stop"))
    if stop_price <= 0 or stop_price >= price:
        stop_pct = _env_float("PB1_BUYABLE_BACKFILL_STOP_PCT", 0.03)
        stop_price = price * (1.0 - max(0.005, min(stop_pct, 0.20)))
    features["entry_price"] = _safe_float(features.get("entry_price"), price) or price
    features["order_price"] = _safe_float(features.get("order_price"), price) or price
    features["stop_price"] = stop_price
    features.setdefault("initial_stop", stop_price)
    features.setdefault("entry_style_selected", features.get("entry_component") or "PULLBACK")
    features.setdefault("entry_reason", "ENTRY_BACKFILL")
    try:
        setattr(cf, "planned_qty", max(1, int(qty)))
    except Exception:
        return False
    if not getattr(cf, "client_order_key", None) and hasattr(engine, "_client_order_key"):
        try:
            stage = engine._entry_stage_name() if hasattr(engine, "_entry_stage_name") else "PB1-CLOSE"
            cf.client_order_key = engine._client_order_key(
                _candidate_code(cf),
                getattr(cf, "mode", 1),
                "BUY",
                str(getattr(engine, "window_label", "day") or "day"),
                stage,
            )
        except Exception as exc:
            _logger.info("[ENTRY][BUYABLE_BACKFILL][KEY_SKIP] code=%s err=%s", _candidate_code(cf), exc)
            return False
    if hasattr(engine, "_build_entry_plan"):
        try:
            stage = engine._entry_stage_name() if hasattr(engine, "_entry_stage_name") else "PB1-CLOSE"
            plan = engine._build_entry_plan(
                cf,
                entry_price=features["entry_price"],
                order_price=features["order_price"],
                stop_price=stop_price,
                trigger_ok=bool(features.get("breakout_trigger_ok") or features.get("trigger_ok")),
                trigger_info=features.get("entry_trigger") if isinstance(features.get("entry_trigger"), dict) else {},
                entry_mode=str(features.get("entry_mode") or "buyable_backfill"),
                stage=stage,
                price_source=str(features.get("price_source") or "buyable_backfill"),
            )
            cf.entry_plan = plan
            features["entry_plan"] = plan
            if hasattr(engine, "_validate_entry_plan"):
                ok, reasons = engine._validate_entry_plan(plan)
                if not ok:
                    _logger.info("[ENTRY][BUYABLE_BACKFILL][PLAN_SKIP] code=%s reasons=%s", _candidate_code(cf), reasons)
                    return False
        except Exception as exc:
            _logger.info("[ENTRY][BUYABLE_BACKFILL][PLAN_SKIP] code=%s err=%s", _candidate_code(cf), exc)
            return False
    return True


def _apply_buyable_candidate_backfill(
    engine: Any,
    *,
    result: Any,
    orderable_candidates: list[Any],
    candidates: list[Any] | None,
    new_position_limit: int = 0,
    target_new_positions: int = 0,
    tick_budget_krw: float = 0.0,
    planned_spent: float = 0.0,
    available_cash_krw: float = 0.0,
    min_order_krw: float = 0.0,
) -> int:
    if not _env_bool("PB1_BUYABLE_BACKFILL_ENABLED", True):
        return 0
    pool = list(candidates or [])
    if orderable_candidates or not pool:
        return 0
    limit_candidates = [int(v or 0) for v in (new_position_limit, target_new_positions) if int(v or 0) > 0]
    target_limit = min(limit_candidates) if limit_candidates else max(1, _safe_int(os.getenv("PB1_BUYABLE_BACKFILL_MAX_CANDIDATES"), 1))
    if target_limit <= 0:
        return 0
    blocked_codes = _collect_buyable_backfill_blocked_codes(engine)
    existing = {_candidate_code(cf) for cf in orderable_candidates}
    planned_running = float(planned_spent or 0.0)
    added = 0
    _logger.info(
        "[ENTRY][BUYABLE_BACKFILL][START] orderable=0 pool=%s target=%s blocked=%s",
        len(pool), target_limit, sorted(blocked_codes),
    )
    ranked = sorted(
        pool,
        key=lambda cf: _safe_float(_candidate_features(cf).get("score_final") or _candidate_features(cf).get("final_score") or _candidate_features(cf).get("score")),
        reverse=True,
    )
    for cf in ranked:
        if added >= target_limit or len(orderable_candidates) >= target_limit:
            break
        code = _candidate_code(cf)
        if not code or code in existing:
            continue
        block_reason = _buyable_backfill_block_reason(engine, cf, blocked_codes=blocked_codes)
        if block_reason:
            _logger.info("[ENTRY][BUYABLE_BACKFILL][SKIP] code=%s reason=%s", code, block_reason)
            continue
        safe, safe_reason = _candidate_safe_for_buyable_backfill(cf)
        if not safe:
            _logger.info("[ENTRY][BUYABLE_BACKFILL][SKIP] code=%s reason=%s", code, safe_reason)
            continue
        features = _candidate_features(cf)
        price = _safe_float(features.get("order_price") or features.get("entry_price") or features.get("close") or features.get("last_close"))
        qty = _safe_int(getattr(cf, "planned_qty", 0) or features.get("planned_qty") or features.get("qty"))
        if qty <= 0:
            if not _env_bool("PB1_BUYABLE_BACKFILL_FORCE_MIN1", True):
                _logger.info("[ENTRY][BUYABLE_BACKFILL][SKIP] code=%s reason=qty_zero", code)
                continue
            qty = 1
        order_value = price * qty
        if min_order_krw > 0 and order_value < float(min_order_krw):
            _logger.info("[ENTRY][BUYABLE_BACKFILL][SKIP] code=%s reason=min_order_krw", code)
            continue
        if available_cash_krw > 0 and order_value > float(available_cash_krw):
            _logger.info("[ENTRY][BUYABLE_BACKFILL][SKIP] code=%s reason=insufficient_cash", code)
            continue
        if tick_budget_krw > 0 and planned_running + order_value > float(tick_budget_krw):
            _logger.info("[ENTRY][BUYABLE_BACKFILL][SKIP] code=%s reason=tick_budget_exceeded", code)
            continue
        if not _ensure_backfill_entry_plan(engine, cf, price=price, qty=qty):
            continue
        try:
            cf.setup_ok = True
        except Exception:
            pass
        features["buyable_ok"] = True
        features["buyable_backfill"] = True
        features["candidate_tier"] = "buyable_backfill"
        features["risk_tag"] = features.get("risk_tag") or "BUYABLE_BACKFILL"
        orderable_candidates.append(cf)
        existing.add(code)
        planned_running += order_value
        added += 1
        _logger.info("[ENTRY][BUYABLE_BACKFILL][ACCEPT] code=%s qty=%s value=%.0f", code, qty, order_value)
    if added:
        try:
            result.orderable_candidates = orderable_candidates
            result.backfill_attempted = True
            result.backfill_added_count = int(getattr(result, "backfill_added_count", 0) or 0) + added
            result.planned_spent_after_backfill = planned_running
        except Exception:
            pass
        _logger.info("[ENTRY][BUYABLE_BACKFILL][DONE] added=%s codes=%s", added, [_candidate_code(c) for c in orderable_candidates])
    else:
        _logger.info("[ENTRY][BUYABLE_BACKFILL][DONE] added=0 reason=no_safe_unblocked_candidate")
    return added


def _wrap_candidate_width_backfill(engine_cls: type[Any]) -> bool:
    if getattr(engine_cls, "_buyable_candidate_backfill_guard_installed", False):
        return False
    original = getattr(engine_cls, "_apply_candidate_width_backfill_and_concentration_guard", None)
    if not callable(original):
        return False

    @functools.wraps(original)
    def guarded(self: Any, *args: Any, **kwargs: Any) -> Any:
        result = original(self, *args, **kwargs)
        try:
            orderable_candidates = getattr(result, "orderable_candidates", None)
            if orderable_candidates is None:
                orderable_candidates = kwargs.get("orderable_candidates") or []
            if not orderable_candidates:
                _apply_buyable_candidate_backfill(
                    self,
                    result=result,
                    orderable_candidates=orderable_candidates,
                    candidates=kwargs.get("candidates") or [],
                    new_position_limit=int(kwargs.get("new_position_limit") or 0),
                    target_new_positions=int(kwargs.get("target_new_positions") or 0),
                    tick_budget_krw=float(kwargs.get("tick_budget_krw") or 0.0),
                    planned_spent=float(kwargs.get("planned_spent") or 0.0),
                    available_cash_krw=float(kwargs.get("available_cash_krw") or 0.0),
                    min_order_krw=float(kwargs.get("min_order_krw") or 0.0),
                )
        except Exception as exc:  # pragma: no cover - trade loop must fail closed, not crash from guard
            _logger.warning("[ENTRY][BUYABLE_BACKFILL][GUARD_FAIL_CLOSED] err=%s", exc)
        return result

    setattr(engine_cls, "_apply_candidate_width_backfill_and_concentration_guard", guarded)
    setattr(engine_cls, "_buyable_candidate_backfill_guard_installed", True)
    return True


def _wrap_pb1_engine_class(engine_cls: type[Any]) -> bool:
    """Make session finalization see the latest real PB1Engine."""

    installed_any = False
    if not getattr(engine_cls, "_engine_runner_finalization_guard_installed", False):
        def _wrap_method(method_name: str) -> None:
            original = getattr(engine_cls, method_name, None)
            if not callable(original):
                return

            @functools.wraps(original)
            def guarded(self: Any, *args: Any, **kwargs: Any) -> Any:
                builtins.engine_runner = self
                try:
                    return original(self, *args, **kwargs)
                finally:
                    builtins.engine_runner = self

            setattr(engine_cls, method_name, guarded)

        _wrap_method("run")
        _wrap_method("run_close_cancel")
        setattr(engine_cls, "_engine_runner_finalization_guard_installed", True)
        installed_any = True
    installed_any = _wrap_candidate_width_backfill(engine_cls) or installed_any
    return installed_any


def _install_pb1_engine_runner_finalization_guard() -> None:
    _ensure_engine_runner_fallback()
    try:
        from trader.pb1_engine import PB1Engine
    except Exception as exc:  # pragma: no cover - keep package import non-fatal
        _logger.warning("[PB1][ENGINE_RUNNER_GUARD][INSTALL_DEFERRED] err=%s", exc)
        return
    installed = _wrap_pb1_engine_class(PB1Engine)
    if installed:
        _logger.info("[PB1][ENGINE_RUNNER_GUARD][INSTALLED] target=PB1Engine")


_install_pb1_engine_runner_finalization_guard()
