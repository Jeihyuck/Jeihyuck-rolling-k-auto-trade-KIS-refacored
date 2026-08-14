# -*- coding: utf-8 -*-
"""US Order Router.

intent를 risk gate에 통과시키고 DB에 저장한 후
DRY_RUN 또는 KIS 모의주문으로 라우팅한다.

처리 순서:
1. intent normalize
2. save_order_intent()
3. DB 기반 + memory 기반 중복 key 합산
4. risk gate 실행
5. BLOCK → mark_order_intent_blocked, return BLOCKED
6. DRY_RUN=1 → save_dry_run_order, mark_order_intent_sent, return DRY_RUN
7. DRY_RUN=0 → KIS 주문 → save_order_ack, mark_order_intent_sent, return ACK
8. KIS REJECT → save_order_reject, mark_order_intent_rejected, return REJECT
"""
from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any

from trader.us import config as us_cfg
from trader.us.execution.risk_gate import RiskGateBlocked, assert_order_allowed
from trader.us.runner.status_contract import is_no_balance_sell_reject
from trader.us.market_state_overlay import FORBIDDEN_HEDGE_SYMBOLS
from trader.us.db.repos import (  # test patch surface
    load_today_order_keys,
    mark_order_intent_blocked,
    mark_order_intent_dry_run,
    save_dry_run_order,
    save_order_intent,
)

logger = logging.getLogger(__name__)
AI_TECH_COMBINED_CLUSTERS = {"AI_SEMI", "AI_SOFTWARE", "DATA_CENTER_POWER", "MEGA_TECH"}


def resolve_entry_metadata_contract_reason(intent: dict, *, required: bool = False) -> str | None:
    """Validate classification metadata without treating explicit OTHER as missing."""
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    candidate_fields = ("theme_cluster", "classification_source", "position_state", "position_action")
    if not required and not any(
        intent.get(key) not in (None, "") or meta.get(key) not in (None, "")
        for key in candidate_fields
    ):
        # Legacy/manual router callers do not carry a Final30 classification
        # contract. PB1 candidates always do and are validated identically in
        # preflight and router.
        return None
    invariant_fields = (
        "source_tags", "sector", "industry", "theme_cluster", "classification_source",
        "trend_score", "score_final", "rank_final30", "market_state", "market_regime",
        "position_state", "position_action",
    )
    if any(
        intent.get(key) is not None
        and meta.get(key) is not None
        and intent.get(key) != meta.get(key)
        for key in invariant_fields
    ):
        return "ENTRY_METADATA_INVARIANT_FAIL"
    required = ("theme_cluster", "position_state", "position_action")
    if any(intent.get(key) in (None, "") and meta.get(key) in (None, "") for key in required):
        return "classification_metadata_missing"
    top_cluster = str(intent.get("theme_cluster") or "").strip().upper()
    meta_cluster = str(meta.get("theme_cluster") or "").strip().upper()
    if top_cluster == "OTHER" and meta_cluster == "OTHER":
        top_source = str(intent.get("classification_source") or "").strip()
        meta_source = str(meta.get("classification_source") or "").strip()
        if not top_source or not meta_source:
            return "classification_metadata_missing"
    return None


def normalize_canonical_risk_snapshot(projected_state: dict, *, allowed_symbols=None, current_position_symbols=None) -> dict:
    """Return the complete, deterministic schema used for preflight/router comparison."""
    now = projected_state.get("now")
    return {
        "available_cash_usd": float(projected_state.get("available_cash_usd") or 0.0),
        "daily_notional_usd": float(projected_state.get("daily_notional_usd") or 0.0),
        "position_count": int(projected_state.get("position_count") or 0),
        "portfolio_equity_usd": float(projected_state.get("portfolio_usd") or 0.0),
        "order_keys": sorted(projected_state.get("order_keys") or []),
        "cluster_exposure": dict(sorted((projected_state.get("cluster_exposure") or {}).items())),
        "cluster_caps_usd": dict(sorted((projected_state.get("cluster_caps_usd") or {}).items())),
        "default_cluster_cap_usd": projected_state.get("default_cluster_cap_usd"),
        "ai_combined_cap_usd": projected_state.get("ai_combined_cap_usd"),
        "now": now.isoformat() if hasattr(now, "isoformat") else str(now or ""),
        "allowed_symbols": sorted(allowed_symbols or []),
        "current_position_symbols": sorted(current_position_symbols or []),
    }


def canonical_risk_snapshot_changed_fields(preflight_state: dict, router_state: dict) -> list[str]:
    """Return every canonical input whose normalized value changed."""
    return sorted(
        key
        for key in set(preflight_state) | set(router_state)
        if preflight_state.get(key) != router_state.get(key)
    )


class _StatusCompat(str):
    def __new__(cls, value: str, *aliases: str):
        obj = str.__new__(cls, value)
        obj.aliases = set(aliases)
        return obj
    def __eq__(self, other):
        return str.__eq__(self, other) or str(other) in self.aliases
    __hash__ = str.__hash__

# in-process 중복 키 (DB fallback 없을 때도 동일 process 내 중복 차단)
_SENT_ORDER_KEYS: set[str] = set()
_BLOCKED_INTENT_KEYS: set[tuple[str, str, str]] = set()
_CASH_EXHAUSTED_TICKS: set[str] = set()
_CASH_UNAVAILABLE_TICKS: set[str] = set()


@dataclass(frozen=True)
class OrderPreflightDecision:
    allowed: bool
    reason: str = ""
    scope: str = "CANDIDATE"
    resized_intent: dict | None = None


class BuyPreflightSession:
    """Stateful but side-effect-free projected-account preflight session."""
    def __init__(self, *, target: int, projected_state: dict, allowed_symbols=None, current_positions=None,
                 available_new_symbol_slots=None, max_new_symbol_buys=None, max_add_to_existing_buys=None):
        self.target = max(0, target)
        self.state = projected_state
        self.allowed_symbols = allowed_symbols
        self.current_positions = current_positions or []
        self.available_new_symbol_slots = available_new_symbol_slots
        self.max_new_symbol_buys = max_new_symbol_buys
        self.max_add_to_existing_buys = max_add_to_existing_buys
        self.accepted: list[dict] = []
        self.accepted_states: dict[str, dict] = {}
        self.rejected: list[dict] = []
        self.attempted = self.new_accepted = self.add_accepted = 0
        self.global_stop_reason = self.system_invariant_failure = ""

    def consider(self, intent: dict) -> OrderPreflightDecision:
        if len(self.accepted) >= self.target or self.global_stop_reason:
            return OrderPreflightDecision(False, "preflight_target_reached", "GLOBAL")
        self.attempted += 1
        action = intent.get("position_action") or (intent.get("meta") or {}).get("position_action")
        is_add = action == "ADD_TO_EXISTING_BUY"
        if is_add and self.max_add_to_existing_buys is not None and self.add_accepted >= self.max_add_to_existing_buys:
            decision = OrderPreflightDecision(False, "max_add_buys_per_tick", "CANDIDATE")
        elif not is_add and (
            (self.available_new_symbol_slots is not None and self.new_accepted >= self.available_new_symbol_slots)
            or (self.max_new_symbol_buys is not None and self.new_accepted >= self.max_new_symbol_buys)
        ):
            decision = OrderPreflightDecision(False, "max_positions_reached_new_symbol", "CANDIDATE")
        else:
            decision = preflight_buy_order(intent, self.state, self.allowed_symbols, self.current_positions)
        if not decision.allowed:
            self.rejected.append({"symbol": intent.get("symbol"), "reason": decision.reason, "scope": decision.scope, "block_stage": "order_preflight"})
            if decision.scope in {"GLOBAL", "SYSTEM"} and decision.reason != "preflight_target_reached":
                self.global_stop_reason = decision.reason
                if decision.scope == "SYSTEM":
                    self.system_invariant_failure = decision.reason
            return decision
        accepted = decision.resized_intent or intent
        identity = str(accepted.get("client_order_key") or accepted.get("order_key") or accepted.get("symbol") or len(self.accepted))
        current_symbols = {str(item.get("symbol") or item.get("code") or "").upper().strip() for item in self.current_positions if isinstance(item, dict)}
        self.accepted_states[identity] = normalize_canonical_risk_snapshot(
            self.state, allowed_symbols=self.allowed_symbols, current_position_symbols=current_symbols,
        )
        self.accepted.append(accepted)
        self.add_accepted += int(is_add)
        self.new_accepted += int(not is_add)
        notional = float(accepted.get("notional_usd") or 0.0)
        self.state["available_cash_usd"] -= notional
        self.state["daily_notional_usd"] += notional
        symbol = str(accepted.get("symbol") or "").upper().strip()
        if not is_add:
            self.state["position_count"] += 1
        key = accepted.get("client_order_key") or accepted.get("order_key")
        if key: self.state.setdefault("order_keys", set()).add(key)
        values = self.state.setdefault("symbol_market_value", {})
        values[symbol] = float(values.get(symbol) or 0) + notional
        cluster = str(accepted.get("theme_cluster") or (accepted.get("meta") or {}).get("theme_cluster") or "UNCLASSIFIED")
        exposures = self.state.setdefault("cluster_exposure", {})
        exposures[cluster] = float(exposures.get(cluster) or 0) + notional
        return decision

    def diagnostics(self) -> dict:
        return {"global_stop_reason": self.global_stop_reason, "system_invariant_failure": self.system_invariant_failure,
                "attempted": self.attempted, "accepted": len(self.accepted), "rejected": len(self.rejected),
                "candidate_pool_exhausted": not self.global_stop_reason and len(self.accepted) < self.target,
                "accepted_states": dict(self.accepted_states)}


_GLOBAL_PREFLIGHT_REASONS = {
    "after_entry_cutoff", "outside_session_window", "prep_contract_not_ok",
    "balance_unavailable", "cash_unavailable", "kis_order_allowed_false",
    "us_agent_not_enabled", "trading_region_not_us", "kis_env_not_practice",
    "strategy_env_not_practice",
}


def _risk_reason(exc: Exception) -> str:
    text = str(exc)
    if "reason=" in text:
        return text.split("reason=", 1)[1].split()[0]
    if "[US_DUPLICATE][BLOCK]" in text:
        return "duplicate_client_order_key"
    return text


def canonical_order_risk_check(intent: dict, projected_state: dict, *, allowed_symbols=None, current_position_symbols=None) -> None:
    """The single side-effect-free risk check shared by preflight and router."""
    if str(intent.get("side") or "BUY").upper() == "BUY":
        metadata_reason = resolve_entry_metadata_contract_reason(intent)
        if metadata_reason:
            raise RiskGateBlocked(f"[US_RISK][BLOCK] symbol={intent.get('symbol')} reason={metadata_reason}")
    symbol = str(intent.get("symbol") or "").upper().strip()
    position_action = intent.get("position_action") or (intent.get("meta") or {}).get("position_action") or ""
    cluster = str(intent.get("theme_cluster") or (intent.get("meta") or {}).get("theme_cluster") or "")
    cluster_caps = projected_state.get("cluster_caps_usd") or {}
    cap = cluster_caps.get(cluster, projected_state.get("default_cluster_cap_usd"))
    if cap is not None:
        current_cluster = float((projected_state.get("cluster_exposure") or {}).get(cluster) or 0.0)
        if current_cluster + float(intent.get("notional_usd") or 0.0) > float(cap):
            raise RiskGateBlocked(f"[US_RISK][BLOCK] symbol={symbol} reason=projected_cluster_cap_exceeded")
    ai_cap = projected_state.get("ai_combined_cap_usd")
    if ai_cap is not None and cluster in AI_TECH_COMBINED_CLUSTERS:
        ai_exposure = sum(float((projected_state.get("cluster_exposure") or {}).get(name) or 0.0) for name in AI_TECH_COMBINED_CLUSTERS)
        if ai_exposure + float(intent.get("notional_usd") or 0.0) > float(ai_cap):
            raise RiskGateBlocked(f"[US_RISK][BLOCK] symbol={symbol} reason=projected_ai_tech_combined_cap_exceeded")
    assert_order_allowed(
        intent,
        current_daily_notional_usd=float(projected_state.get("daily_notional_usd") or 0),
        current_filled_notional=float(projected_state.get("filled_notional_usd") or 0.0),
        current_acknowledged_notional=float(projected_state.get("acknowledged_notional_usd") or 0.0),
        current_pending_notional=float(projected_state.get("pending_notional_usd") or 0.0),
        current_reserved_notional=float(projected_state.get("reserved_notional_usd") or 0.0),
        current_risk_total_notional=float(projected_state.get("risk_total_notional_usd") or projected_state.get("daily_notional_usd") or 0.0),
        current_position_count=int(projected_state.get("position_count") or 0),
        total_portfolio_usd=float(projected_state.get("portfolio_usd") or 0),
        available_cash_usd=float(projected_state.get("available_cash_usd") or 0),
        existing_order_keys=set(projected_state.get("order_keys") or set()),
        now=projected_state.get("now"),
        allowed_symbols=allowed_symbols,
        current_position_symbols=current_position_symbols,
        trade_date=intent.get("trade_date"),
        is_existing_position_buy=position_action == "ADD_TO_EXISTING_BUY" or symbol in set(current_position_symbols or set()),
    )


def preflight_buy_order(intent: dict, projected_state: dict, allowed_symbols=None, current_positions=None) -> OrderPreflightDecision:
    """Run canonical BUY risk checks without persistence or broker submission."""
    if str(intent.get("side") or "BUY").upper() != "BUY":
        return OrderPreflightDecision(True, resized_intent=dict(intent))
    if projected_state.get("available_cash_usd") is None:
        return OrderPreflightDecision(False, "cash_unavailable", "GLOBAL")
    if float(projected_state.get("portfolio_usd") or 0.0) <= 0 and (
        projected_state.get("default_cluster_cap_usd") is not None or projected_state.get("ai_combined_cap_usd") is not None
    ):
        return OrderPreflightDecision(False, "portfolio_equity_unavailable", "SYSTEM")
    metadata_reason = resolve_entry_metadata_contract_reason(intent, required=True)
    if metadata_reason:
        scope = "SYSTEM" if metadata_reason == "ENTRY_METADATA_INVARIANT_FAIL" else "CANDIDATE"
        return OrderPreflightDecision(False, metadata_reason, scope)
    current_symbols = {
        str(item.get("symbol") or item.get("code") or item).upper().strip()
        for item in current_positions or []
    }
    try:
        canonical_order_risk_check(intent, projected_state, allowed_symbols=allowed_symbols, current_position_symbols=current_symbols)
    except RiskGateBlocked as exc:
        reason = _risk_reason(exc)
        if reason == "daily_notional_exceeded" and float(projected_state.get("daily_notional_usd") or 0) >= float(os.getenv("US_MAX_DAILY_NOTIONAL_USD", "500")):
            return OrderPreflightDecision(False, reason, "GLOBAL")
        if reason in {"cash_below_buffer", "us_capital_budget_exceeded"}:
            buffer = float(os.getenv("US_MIN_CASH_BUFFER_USD", "50"))
            if float(projected_state.get("available_cash_usd") or 0) <= buffer:
                return OrderPreflightDecision(False, reason, "GLOBAL")
        return OrderPreflightDecision(False, reason, "GLOBAL" if reason in _GLOBAL_PREFLIGHT_REASONS else "CANDIDATE")
    except Exception as exc:
        logger.exception("[US_ENTRY][PREFLIGHT_SYSTEM_ERROR] symbol=%s", intent.get("symbol"))
        return OrderPreflightDecision(False, f"preflight_system_error:{type(exc).__name__}", "SYSTEM")
    return OrderPreflightDecision(True, resized_intent=dict(intent))


def select_preflight_buy_candidates(
    intents: list[dict],
    *,
    target_accept_count: int,
    projected_state: dict,
    allowed_symbols=None,
    current_positions=None,
    available_new_symbol_slots: int | None = None,
    max_new_symbol_buys: int | None = None,
    max_add_to_existing_buys: int | None = None,
) -> tuple[list[dict], list[dict], dict]:
    """Fill final BUY slots using cumulative, side-effect-free risk checks."""
    session = BuyPreflightSession(target=target_accept_count, projected_state=projected_state,
        allowed_symbols=allowed_symbols, current_positions=current_positions,
        available_new_symbol_slots=available_new_symbol_slots, max_new_symbol_buys=max_new_symbol_buys,
        max_add_to_existing_buys=max_add_to_existing_buys)
    for intent in intents or []:
        if len(session.accepted) >= max(0, target_accept_count) or session.global_stop_reason:
            break
        session.consider(intent)
    diagnostics = session.diagnostics()
    return ([] if diagnostics["system_invariant_failure"] else session.accepted), session.rejected, diagnostics


def _kis_env() -> str:
    return (os.getenv("KIS_ENV") or "practice").strip().lower() or "practice"


def _is_cash_insufficient_reject(msg: str) -> bool:
    text = str(msg or "").lower()
    return any(token in text for token in ("주문가능금액 부족", "주문가능금액부족", "insufficient cash", "ord psbl", "cash insufficient"))


def _cash_tick_key(trade_date: str | None, intent: dict | None = None) -> str:
    meta = (intent or {}).get("meta") if isinstance(intent, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    session = str((intent or {}).get("session") or meta.get("session") or os.getenv("PB1_SESSION") or os.getenv("WSL_RUN_SESSION") or "unknown")
    run_id = str((intent or {}).get("run_id") or meta.get("run_id") or os.getenv("US_RUN_ID") or os.getenv("GITHUB_RUN_ID") or "local")
    tick = str((intent or {}).get("tick_id") or (intent or {}).get("tick_seq") or meta.get("tick_id") or meta.get("tick_seq") or os.getenv("US_TICK_ID") or os.getenv("US_TICK_SEQ") or "unknown")
    return "|".join([str(trade_date or "unknown"), session, run_id, tick])


def _client_method(kis_client: Any, name: str) -> Any | None:
    import inspect
    try:
        inspect.getattr_static(kis_client, name)
    except AttributeError:
        return None
    method = getattr(kis_client, name, None)
    return method if callable(method) else None


def _parse_orderable_cash_output(output: dict, *, symbol: str = "", account_env: str = "") -> float | None:
    keys = (
        "ord_psbl_frcr_amt", "frcr_ord_psbl_amt1", "frcr_ord_psbl_amt",
        "ord_psbl_cash", "ovrs_ord_psbl_amt", "orderable_cash", "cash",
        "psbl_amt", "buy_ord_psbl_amt", "max_buy_amt",
    )
    if not isinstance(output, dict):
        logger.warning("[US_ORDER][BROKER_CASH_PARSE_WARN] env=%s symbol=%s reason=output_not_dict", account_env, symbol)
        return None
    for key in keys:
        val = output.get(key)
        if val not in (None, ""):
            try:
                return float(str(val).replace(",", ""))
            except (TypeError, ValueError):
                logger.warning("[US_ORDER][BROKER_CASH_PARSE_WARN] env=%s symbol=%s key=%s value=%s reason=not_numeric", account_env, symbol, key, val)
                return None
    logger.warning("[US_ORDER][BROKER_CASH_PARSE_WARN] env=%s symbol=%s keys=%s reason=orderable_cash_key_missing", account_env, symbol, sorted(output.keys()))
    return None


def _has_broker_orderable_cash_method(kis_client: Any) -> bool:
    return _client_method(kis_client, "get_orderable_cash") is not None or _client_method(kis_client, "get_us_orderable_cash") is not None


def _get_broker_orderable_cash(kis_client: Any, symbol: str, exchange: str, price: float, account_env: str = "") -> float | None:
    method = _client_method(kis_client, "get_orderable_cash")
    if method is not None:
        try:
            return float(method(symbol=symbol, exchange=exchange, price=price) or 0.0)
        except (TypeError, ValueError) as exc:
            logger.warning("[US_ORDER][BROKER_CASH_PARSE_WARN] env=%s symbol=%s reason=data_provider_value_invalid err=%s", account_env, symbol, exc)
            return None
    method = _client_method(kis_client, "get_us_orderable_cash")
    if method is not None:
        raw = method(symbol=symbol, exchange=exchange, price=price)
        output = raw.get("output", raw) if isinstance(raw, dict) else {}
        return _parse_orderable_cash_output(output, symbol=symbol, account_env=account_env)
    logger.warning("[US_ORDER][BROKER_CASH_PARSE_WARN] env=%s symbol=%s reason=orderable_cash_method_missing", account_env, symbol)
    return None


def _get_broker_position(kis_client: Any, symbol: str) -> dict | None:
    method = _client_method(kis_client, "get_balance")
    if method is not None:
        bal = method(force_refresh=True)
    else:
        raw_method = _client_method(kis_client, "get_us_balance")
        if raw_method is None:
            return None
        from trader.us.data_provider import normalize_us_balance
        bal = normalize_us_balance(raw_method(force_refresh=True))
    for pos in bal.get("positions", []) if isinstance(bal, dict) else []:
        if str(pos.get("symbol") or "").upper().strip() == str(symbol or "").upper().strip():
            return pos
    return {}


def _normalize_exchange_code(exchange: str) -> str:
    ex = str(exchange or "").upper().strip()
    aliases = {
        "NAS": "NASDAQ",
        "NASD": "NASDAQ",
        "NASDAQ": "NASDAQ",
        "NYS": "NYSE",
        "NYSE": "NYSE",
        "AMS": "AMEX",
        "AMEX": "AMEX",
        "ASE": "AMEX",
    }
    return aliases.get(ex, ex)


def _lookup_nested_exchange(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    for key in ("exchange", "exch", "market", "ovrs_excg_cd", "tr_mket_name"):
        val = _normalize_exchange_code(obj.get(key))
        if val:
            return val
    return ""


def enrich_sell_exchange(intent: dict, kis_client: Any = None, context: Any = None) -> str:
    """Fill SELL exchange before risk gate so exit intents are not blocked."""
    symbol = str(intent.get("symbol") or "").upper().strip()
    candidates = [
        intent.get("exchange"),
        (getattr(context, "exchange_by_symbol", {}) or {}).get(symbol),
        _lookup_nested_exchange((getattr(context, "positions_by_symbol", {}) or {}).get(symbol, {})),
        _lookup_nested_exchange(intent.get("current_holding")),
        _lookup_nested_exchange(intent.get("kis_balance_position")),
        _lookup_nested_exchange(intent.get("locked_watchlist_row")),
    ]
    # A tick context is authoritative and deliberately prevents N balance calls
    # for N sell intents. Legacy one-off callers retain the broker fallback.
    if kis_client is not None and context is None:
        try:
            candidates.append(_lookup_nested_exchange(_get_broker_position(kis_client, symbol) or {}))
        except Exception as exc:
            logger.warning("[US_EXIT_INTENT][EXCHANGE_LOOKUP_WARN] symbol=%s err=%s", symbol, exc)
    try:
        from trader.us.symbols import resolve_exchange
        candidates.append(resolve_exchange(symbol))
    except Exception:
        pass
    static_map = {"SPY": "NYSE", "DIA": "NYSE", "IWM": "NYSE", "QQQ": "NASDAQ", "QQQM": "NASDAQ", "SMH": "NASDAQ", "SOXX": "NASDAQ"}
    candidates.append(static_map.get(symbol, ""))
    for ex in candidates:
        ex = _normalize_exchange_code(ex)
        if ex:
            intent["exchange"] = ex
            return ex
    logger.error("[US_EXIT_INTENT][EXCHANGE_MISSING_FATAL] symbol=%s", symbol)
    return ""


def resolve_dry_run_for_us_order() -> bool:
    """US order DRY_RUN 여부를 resolve하고 runtime guard 검증.
    
    Rules:
    - KIS_ENV=practice + DRY_RUN=0: ALLOWED (practice 주문)
    - KIS_ENV!=practice + DRY_RUN=0: FORBIDDEN (즉시 RuntimeError)
    - DRY_RUN=1: dry-run mode
    
    Returns:
        True: DRY_RUN mode
        False: Real order mode (practice orders are allowed)
        
    Raises:
        RuntimeError: KIS_ENV!=practice에서 DRY_RUN=0 시도 시
    """
    from trader.utils.env import env_bool
    
    kis_env = str(os.getenv("KIS_ENV", "")).strip().lower()
    dry_run_raw = os.getenv("DRY_RUN", "1").strip()
    
    dry_run = env_bool("DRY_RUN", default=True)
    
    logger.info(
        "[US_ORDER][DRY_RUN_RESOLVE] kis_env=%s raw=%s resolved=%d",
        kis_env, dry_run_raw, int(dry_run)
    )
    
    # Runtime safety: KIS_ENV != practice에서 DRY_RUN=0은 허용하지 않음
    if kis_env not in ("practice", "vps") and not dry_run:
        msg = (
            f"[US_ORDER][DRY_RUN_RESOLVE][FORBIDDEN] "
            f"DRY_RUN=0 is only allowed when KIS_ENV=practice, got KIS_ENV={kis_env}"
        )
        logger.error(msg)
        raise RuntimeError(msg)
    
    return dry_run



def _pending_sell_qty_for_symbol(symbol: str, trade_date: str | None) -> int:
    """Best-effort pending SELL quantity for available_to_sell sizing."""
    statuses = {"ACK", "SUBMITTED", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING", "ACK_DB_FAILED"}
    try:
        from trader.us.db.repos import load_us_daily_orders_for_report
        total = 0
        for row in load_us_daily_orders_for_report(trade_date or "") or []:
            if str(row.get("symbol") or "").upper().strip() != str(symbol or "").upper().strip():
                continue
            if str(row.get("side") or "").upper() != "SELL":
                continue
            if str(row.get("status") or "").upper() not in statuses:
                continue
            total += int(row.get("qty") or row.get("order_qty") or row.get("quantity") or 0)
        return max(0, total)
    except Exception as exc:
        logger.warning("[US_ORDER][PENDING_SELL_QTY][WARN] symbol=%s trade_date=%s err=%s", symbol, trade_date, exc)
        return 0

def route_order(
    intent: dict,
    *,
    current_daily_notional_usd: float = 0.0,
    current_filled_notional_usd: float = 0.0,
    current_acknowledged_notional_usd: float = 0.0,
    current_pending_notional_usd: float = 0.0,
    current_reserved_notional_usd: float = 0.0,
    current_risk_total_notional_usd: float = 0.0,
    current_position_count: int = 0,
    total_portfolio_usd: float = 1000.0,
    available_cash_usd: float = 1000.0,
    kis_client: Any | None = None,
    signal_only: bool = False,
    kis_order_allowed: bool = True,
    allowed_symbols: "set[str] | None" = None,
    current_position_symbols: "set[str] | None" = None,
    context: Any | None = None,
    now: Any | None = None,
    projected_cluster_exposure: dict | None = None,
    cluster_caps_usd: dict | None = None,
    default_cluster_cap_usd: float | None = None,
    ai_combined_cap_usd: float | None = None,
    projected_order_keys: set[str] | None = None,
) -> dict:
    """Order intent를 라우팅한다.

    Args:
        kis_order_allowed: If False, return ORDER_DISABLED status
        allowed_symbols: BUY 허용 심볼 집합 (locked watchlist)
        current_position_symbols: SELL universe (현재 보유 포지션 심볼 집합)

    Returns:
        {"status": "DRY_RUN"|"ACK"|"BLOCKED"|"REJECT"|"SIGNAL_ONLY"|"ORDER_DISABLED", ...}
    """
    # This check deliberately precedes identity normalization, persistence and
    # every KIS call: malformed legacy intents cannot bypass symbol ownership.
    from trader.us.strategy_ownership import is_valid_tqqq_intent, owner_for_symbol, TQQQ_OWNER
    if owner_for_symbol(intent.get("symbol")) == TQQQ_OWNER and not is_valid_tqqq_intent(intent):
        logger.error(
            "[TQQQ_INF][OWNERSHIP_REJECT] symbol=TQQQ strategy_owner=%s sleeve_id=%s",
            intent.get("strategy_owner"), intent.get("sleeve_id"),
        )
        return {"status": "BLOCKED", "reason": "tqqq_ownership_rejected",
                "broker_submit": False, "intent": intent}

    from trader.us.db.repos import (
        save_order_intent, save_dry_run_order, save_order_ack, save_order_reject,
        mark_order_intent_sent, mark_order_intent_blocked, mark_order_intent_rejected,
        mark_order_intent_dry_run,
        load_today_order_keys,
    )
    from trader.utils.env import env_bool

    from trader.us.execution.order_identity import InvalidOrderIdentity, normalize_and_validate_order_identity
    try:
        intent = normalize_and_validate_order_identity(intent, context)
    except InvalidOrderIdentity as exc:
        logger.critical("[US_ORDER][INVALID_ORDER_IDENTITY] error=%s", exc)
        return {"status": "INVALID_ORDER_IDENTITY", "reason": str(exc), "broker_submit": False, "intent": intent}
    # Persist an explicit attribution envelope for every routed intent. Legacy
    # callers are standard-owned; dedicated symbols were already validated.
    intent = dict(intent)
    intent.setdefault("strategy_owner", "US_STANDARD")
    intent.setdefault("strategy_name", str(intent.get("strategy") or "US_STANDARD"))
    intent.setdefault("strategy_version", str((intent.get("meta") or {}).get("strategy_version") or "LEGACY"))
    intent.setdefault("sleeve_id", intent["strategy_owner"])
    symbol = intent.get("symbol", "")
    side = str(intent.get("side", "BUY")).upper()
    symbol_upper = str(symbol or "").upper().strip()
    position_action = (
        intent.get("position_action")
        or (intent.get("meta") or {}).get("position_action")
        or ""
    )
    current_position_symbols_upper = {str(s).upper().strip() for s in current_position_symbols or set()}
    is_existing_position_buy = (
        side == "BUY"
        and (
            position_action == "ADD_TO_EXISTING_BUY"
            or (current_position_symbols is not None and symbol_upper in current_position_symbols_upper)
        )
    )
    logger.info(
        "[US_ORDER][POSITION_ACTION] symbol=%s side=%s position_action=%s is_existing_position_buy=%d",
        symbol_upper, side, position_action, int(is_existing_position_buy),
    )
    qty = int(intent.get("qty", 0))
    price = float(intent.get("limit_price", 0.0))
    exchange = intent.get("exchange") or ("NASDAQ" if side == "BUY" else "")
    if side == "SELL":
        exchange = enrich_sell_exchange(intent, kis_client, context)
        if not exchange:
            return {"status": "EXCHANGE_MISSING_FATAL", "reason": "sell_exchange_unresolved",
                    "broker_submit": False, "retry_order": False, "requires_reconcile": False, "intent": intent}
    if context is not None and (symbol_upper, side) in context.blocked_symbol_sides:
        return {"status": "ORDER_FENCED_BEFORE_BROKER_SUBMIT", "reason": "unresolved_prior_order",
                "broker_submit": False, "retry_order": False, "requires_reconcile": True, "intent": intent}
    order_key = intent.get("client_order_key") or intent.get("order_key", "")
    trade_date = intent.get("trade_date")
    if not str(trade_date or "").strip():
        return {"status": "INVALID_ORDER_IDENTITY", "reason": "trade_date_required_before_broker_submit",
                "broker_submit": False, "retry_order": False, "requires_reconcile": False, "intent": intent}

    def _persist_with_trade_date(func, payload):
        return func(payload, trade_date=trade_date)

    logger.info(
        "[US_ORDER][INTENT] symbol=%s side=%s qty=%s notional_usd=%.2f key=%s",
        symbol, side, qty, float(intent.get("notional_usd", 0)), order_key,
    )

    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    intent["submit_attempt_id"] = str(intent.get("submit_attempt_id") or uuid.uuid4())
    meta["submit_attempt_id"] = intent["submit_attempt_id"]
    intent["meta"] = meta
    if side == "SELL" and str(intent.get("reason") or meta.get("reason") or "").startswith("TAKE_PROFIT"):
        from trader.us.profit_capture import authoritative_broker_avg, as_decimal, calc_return_rate
        try:
            broker_avg, _ = authoritative_broker_avg({**meta, "qty": qty, "orderable_qty": intent.get("available_qty", qty), "position_lifecycle_id": intent.get("position_lifecycle_id") or meta.get("position_lifecycle_id")})
            executable = as_decimal(intent.get("limit_price"), name="executable_price")
            threshold = as_decimal(meta.get("tp_threshold_fraction"), name="tp_threshold")
            actual_return = calc_return_rate(executable, broker_avg)
            if actual_return <= 0 or actual_return < threshold:
                raise ValueError("threshold_not_met" if actual_return > 0 else "non_positive_return")
        except ValueError as exc:
            logger.warning("[US_PROFIT_CAPTURE][PRE_SUBMIT_GUARD] symbol=%s decision=BLOCK reason=%s", symbol_upper, exc)
            return {"status": "BLOCKED", "reason": "take_profit_pre_submit_guard_failed", "guard_reason": str(exc), "broker_submit": False, "intent": intent}
    hard_block_reasons = {
        "FORBIDDEN_HEDGE_OR_INVERSE_ETF",
        "DEFENSE_CRASH_ENTRY_BLOCK",
        "DEFENSE_RISK_OFF_AI_TECH_BLOCK",
        "PREP_CONTRACT_TRADE_BLOCK",
        "MARKET_STATE_ENTRY_BLOCK",
        "BLOCKED_CLUSTER_EXPOSURE",
    }
    blocked_reason = str(intent.get("blocked_reason") or meta.get("blocked_reason") or "")
    if side == "BUY" and symbol_upper in FORBIDDEN_HEDGE_SYMBOLS:
        blocked_reason = "FORBIDDEN_HEDGE_OR_INVERSE_ETF"
    if side == "BUY" and (blocked_reason in hard_block_reasons or qty <= 0 or float(intent.get("notional_usd") or 0) <= 0):
        if not blocked_reason:
            blocked_reason = "invalid_buy_qty_or_notional"
        logger.warning("[US_ORDER][BUY_BLOCKED] symbol=%s reason=%s", symbol_upper, blocked_reason)
        return {"status": "BLOCKED", "reason": blocked_reason, "symbol": symbol, "side": side, "qty": qty, "intent": intent}

    # KIS order disabled
    if not kis_order_allowed:
        logger.info(
            "[US_ORDER][DISABLED] symbol=%s side=%s qty=%s reason=kis_order_allowed_false",
            symbol, side, qty,
        )
        return {
            "status": "ORDER_DISABLED",
            "reason": "kis_order_allowed_false",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "intent": intent,
        }

    # Signal-only mode: 신호만 생성, KIS 주문 차단
    if signal_only:
        logger.info(
            "[US_ORDER][SIGNAL_ONLY] symbol=%s side=%s qty=%s reason=KIS_ORDER_DISABLED_SIGNAL_ONLY",
            symbol, side, qty,
        )
        return {
            "status": "SIGNAL_ONLY",
            "reason": "KIS_ORDER_DISABLED_SIGNAL_ONLY",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "intent": intent,
        }

    # 1. SELL duplicate precheck before saving intent: never grow blocked duplicate intents.
    try:
        db_keys = load_today_order_keys(trade_date=trade_date)
    except TypeError:
        db_keys = load_today_order_keys()
    except Exception:
        db_keys = set()
    existing_keys = _SENT_ORDER_KEYS | db_keys | set(projected_order_keys or set())
    if side == "SELL" and order_key and order_key in existing_keys:
        logger.info(
            "[US_ORDER][DEDUP_PRECHECK] symbol=%s side=SELL key=%s action=skip_before_save_intent",
            symbol, order_key,
        )
        return {
            "status": "WARN_DUPLICATE_EXIT_BLOCKED",
            "reason": "duplicate_sell_client_order_key",
            "symbol": symbol,
            "side": side,
            "duplicate_blocked": True,
            "intent": intent,
        }

    # 2. intent DB 저장
    if not _persist_with_trade_date(save_order_intent, intent):
        return {"status": "INVALID_ORDER_IDENTITY", "reason": "intent_persistence_rejected", "broker_submit": False, "intent": intent}

    # 3. 중복 key: DB + in-memory 합산

    # 3. Risk Gate
    gate_intent = {**intent, "exchange": exchange, "client_order_key": order_key, "position_action": position_action}
    gate_state = {
        "daily_notional_usd": current_daily_notional_usd,
        "filled_notional_usd": current_filled_notional_usd,
        "acknowledged_notional_usd": current_acknowledged_notional_usd,
        "pending_notional_usd": current_pending_notional_usd,
        "reserved_notional_usd": current_reserved_notional_usd,
        "risk_total_notional_usd": current_risk_total_notional_usd or current_daily_notional_usd,
        "position_count": current_position_count,
        "portfolio_usd": total_portfolio_usd,
        "available_cash_usd": available_cash_usd,
        "order_keys": existing_keys,
        "now": now,
        "cluster_exposure": projected_cluster_exposure or {},
        "cluster_caps_usd": cluster_caps_usd or {},
        "default_cluster_cap_usd": default_cluster_cap_usd,
        "ai_combined_cap_usd": ai_combined_cap_usd,
    }
    gate_snapshot = normalize_canonical_risk_snapshot(
        gate_state,
        allowed_symbols=allowed_symbols,
        current_position_symbols=current_position_symbols,
    )
    try:
        canonical_order_risk_check(
            gate_intent,
            gate_state,
            allowed_symbols=allowed_symbols,
            current_position_symbols=current_position_symbols,
        )
    except RiskGateBlocked as exc:
        logger.warning("[US_ORDER][BLOCKED] %s", exc)
        logger.warning(
            "[US_ENTRY_BLOCKED] symbol=%s reason=%s current_filled_notional=%.4f current_acknowledged_notional=%.4f current_pending_notional=%.4f current_reserved_notional=%.4f current_risk_total_notional=%.4f new_notional=%.4f",
            symbol,
            _risk_reason(exc),
            float(gate_state.get("filled_notional_usd") or 0.0),
            float(gate_state.get("acknowledged_notional_usd") or 0.0),
            float(gate_state.get("pending_notional_usd") or 0.0),
            float(gate_state.get("reserved_notional_usd") or 0.0),
            float(gate_state.get("risk_total_notional_usd") or 0.0),
            float(intent.get("notional_usd") or 0.0),
        )
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # A안: notional_exceeds_order_limit이면 qty 축소 후 1회 재시도
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        exc_str = str(exc)
        if "notional_exceeds_order_limit" in exc_str:
            logger.warning(
                "[US_ORDER][RESIZE_RETRY] attempting qty resize symbol=%s old_qty=%d",
                symbol, qty,
            )
            
            # US_MAX_ORDER_USD 기준으로 새 qty 계산
            order_cap_usd = float(os.getenv("US_MAX_ORDER_USD", "2500"))
            new_qty = int(order_cap_usd // price) if price > 0 else 0
            
            if new_qty > 0 and new_qty < qty:
                # 축소된 qty로 재시도
                resized_intent = {**intent}
                resized_intent["qty"] = new_qty
                resized_intent["notional_usd"] = new_qty * price
                
                logger.info(
                    "[US_ORDER][RESIZE_RETRY] symbol=%s old_qty=%d new_qty=%d cap=%.2f",
                    symbol, qty, new_qty, order_cap_usd,
                )
                
                # risk gate 재시도
                resized_gate_intent = {**resized_intent, "client_order_key": order_key, "position_action": position_action}
                try:
                    canonical_order_risk_check(
                        resized_gate_intent,
                        gate_state,
                        allowed_symbols=allowed_symbols,
                        current_position_symbols=current_position_symbols,
                    )
                    
                    # 재시도 성공: 축소된 intent로 계속 진행
                    logger.info(
                        "[US_ORDER][RESIZE_RETRY][SUCCESS] symbol=%s new_qty=%d new_notional=%.2f",
                        symbol, new_qty, new_qty * price,
                    )
                    intent = resized_intent  # 원래 intent를 축소된 것으로 교체
                    qty = new_qty  # 로컬 변수도 업데이트
                    
                except RiskGateBlocked as exc2:
                    # 재시도도 실패
                    logger.warning(
                        "[US_ORDER][RESIZE_RETRY][BLOCKED] symbol=%s new_qty=%d reason=%s",
                        symbol, new_qty, exc2,
                    )
                    if order_key:
                        mark_order_intent_blocked(order_key, reason=f"resize_retry_blocked: {exc2}")
                    return {"status": "BLOCKED", "reason": f"resize_retry_blocked: {exc2}", "intent": resized_intent, "canonical_risk_state": gate_snapshot}
            else:
                # 축소해도 qty가 0 이하거나 원래 qty와 같음
                logger.warning(
                    "[US_ORDER][RESIZE_RETRY][SKIP] symbol=%s new_qty=%d old_qty=%d price=%.2f cap=%.2f",
                    symbol, new_qty, qty, price, order_cap_usd,
                )
                if order_key:
                    mark_order_intent_blocked(order_key, reason=str(exc))
                return {"status": "BLOCKED", "reason": str(exc), "intent": intent, "canonical_risk_state": gate_snapshot}
        else:
            # notional_exceeds_order_limit가 아닌 다른 block reason
            reason_text = str(exc)
            reason_code = reason_text.split("reason=", 1)[1].split()[0] if "reason=" in reason_text else reason_text
            if side == "SELL" and reason_code == "pending_sell_order_exists":
                try:
                    from trader.us.db.repos import find_recent_sell_ack, load_us_positions_by_symbols
                    recent_ack = find_recent_sell_ack(symbol=symbol, trade_date=trade_date)
                    positions = load_us_positions_by_symbols([symbol]) if symbol else {}
                    pos = positions.get(symbol, {}) if isinstance(positions, dict) else {}
                    qty_now = int(pos.get("qty") or pos.get("holding_qty") or pos.get("orderable_qty") or 0) if pos else 0
                    if recent_ack and qty_now <= 0:
                        logger.warning(
                            "[US_ORDER][SELL_PENDING][RECENT_ACK_CLOSED] symbol=%s order_no=%s reason=%s",
                            symbol, recent_ack.get("order_no"), reason_code,
                        )
                        return {
                            "status": "OK_EXIT_POSITION_CLOSED",
                            "reason": "pending_sell_order_after_recent_ack_position_closed",
                            "symbol": symbol,
                            "side": side,
                            "requires_reconcile": False,
                            "reconciled": True,
                            "intent": intent,
                            "recent_sell_ack": recent_ack,
                        }
                except Exception as pending_exc:
                    logger.warning("[US_ORDER][SELL_PENDING][RECENT_ACK_WARN] symbol=%s err=%s", symbol, pending_exc)
            dedup_key = (str(symbol).upper(), str(side).upper(), reason_code)
            duplicate_blocked = dedup_key in _BLOCKED_INTENT_KEYS or (side == "SELL" and ("duplicate" in reason_code or "pending_order_exists" in reason_code or "pending_sell_order_exists" in reason_code))
            if duplicate_blocked:
                logger.info(
                    "[US_ORDER][DEDUP] symbol=%s side=%s action=skip_duplicate_blocked_intent reason=%s",
                    symbol, side, reason_code,
                )
            else:
                _BLOCKED_INTENT_KEYS.add(dedup_key)
                if order_key:
                    mark_order_intent_blocked(order_key, reason=reason_text)
            blocked_status = "WARN_DUPLICATE_EXIT_BLOCKED" if (side == "SELL" and duplicate_blocked) else "BLOCKED"
            return {"status": blocked_status, "reason": reason_text, "duplicate_blocked": duplicate_blocked, "intent": intent, "canonical_risk_state": gate_snapshot}

    # 4. DRY_RUN resolve with runtime guard
    dry_run_resolved = resolve_dry_run_for_us_order()
    
    if dry_run_resolved:
        logger.info("[US_ORDER][DRY_RUN] symbol=%s side=%s qty=%s", symbol, side, qty)
        dry_intent = {**intent, "client_order_key": order_key}
        _persist_with_trade_date(save_dry_run_order, dry_intent)
        if order_key:
            # Mark as DRY_RUN instead of SENT to distinguish from real orders
            mark_order_intent_dry_run(order_key)
            _SENT_ORDER_KEYS.add(order_key)
        return {
            "status": "DRY_RUN",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "intent": intent,
        }

    # Capture BUY pre-order position qty before KIS ACK so balance reconciliation
    # can verify a same-symbol position delta instead of treating existing holdings
    # as proof of fill.
    if side == "BUY":
        intent.setdefault("meta", {})
        if isinstance(intent.get("meta"), dict):
            meta = intent["meta"]
            if "pre_order_position_qty" not in meta:
                try:
                    from trader.us.db.repos import load_us_positions_by_symbols

                    db_positions = load_us_positions_by_symbols([symbol], as_of=trade_date) if symbol else {}
                    db_pos = db_positions.get(symbol, {})
                    if db_pos:
                        pre_qty = int(db_pos.get("qty") or db_pos.get("holding_qty") or 0)
                        pre_source = db_pos.get("balance_source") or db_pos.get("entry_price_source") or "us_positions"
                    else:
                        # Only a successful DB lookup proving the symbol is absent may mark
                        # the order as a new-position BUY with pre_order_position_qty=0.
                        pre_qty = 0
                        pre_source = "db_position_absent"
                    meta["pre_order_position_qty"] = pre_qty
                    meta["pre_order_position_source"] = pre_source
                    meta["was_new_position_before_order"] = pre_qty == 0
                except Exception as db_exc:
                    # Lookup failure is not proof of no existing position. Do not write
                    # pre_order_position_qty/was_new_position_before_order, otherwise
                    # BUY balance reconcile could convert existing holdings into a false fill.
                    meta["pre_order_position_source"] = "lookup_failed"
                    logger.warning(
                        "[US_ORDER][BUY_PRE_POSITION][WARN] symbol=%s error=%s",
                        symbol,
                        db_exc,
                    )

    # 5. Paper order via KIS
    if kis_client is None:
        from trader.us.execution.kis_us_client import KisUSClient
        kis_client = KisUSClient(env=_kis_env())

    account_env = _kis_env()
    logger.info("[US_ORDER][BROKER_ENV] env=%s symbol=%s side=%s", account_env, symbol, side)

    if side == "BUY":
        tick_key = _cash_tick_key(trade_date, intent)
        if tick_key in _CASH_EXHAUSTED_TICKS:
            if order_key:
                mark_order_intent_blocked(order_key, reason="broker_orderable_cash_insufficient")
            return {"status": "BLOCKED", "reason": "broker_orderable_cash_insufficient", "cash_exhausted": True, "symbol": symbol, "side": side, "qty": qty, "intent": intent}
        if tick_key in _CASH_UNAVAILABLE_TICKS:
            if order_key:
                mark_order_intent_blocked(order_key, reason="broker_orderable_cash_unavailable")
            return {"status": "BLOCKED", "reason": "broker_orderable_cash_unavailable", "cash_exhausted": False, "symbol": symbol, "side": side, "qty": qty, "intent": intent}
        safety_buffer = float(os.getenv("US_BROKER_ORDERABLE_CASH_SAFETY_BUFFER", "1.01") or 1.01)
        broker_cash = _get_broker_orderable_cash(kis_client, symbol, exchange, price, account_env=account_env)
        required_cash = float(qty) * float(price) * safety_buffer
        if broker_cash is None:
            logger.warning("[US_ORDER][BROKER_CASH_CHECK][SKIP] env=%s symbol=%s reason=broker_orderable_cash_unavailable", account_env, symbol)
            if _has_broker_orderable_cash_method(kis_client):
                _CASH_UNAVAILABLE_TICKS.add(tick_key)
                if order_key:
                    mark_order_intent_blocked(order_key, reason="broker_orderable_cash_unavailable")
                logger.error("[US_ORDER][BUY_BLOCKED] env=%s symbol=%s reason=broker_orderable_cash_unavailable cash_exhausted=0", account_env, symbol)
                return {"status": "BLOCKED", "reason": "broker_orderable_cash_unavailable", "cash_exhausted": False, "symbol": symbol, "side": side, "qty": qty, "intent": intent}
        else:
            logger.info("[US_ORDER][BROKER_CASH_CHECK] env=%s symbol=%s qty=%s price=%.4f broker_orderable_cash=%.2f required_cash=%.2f", account_env, symbol, qty, price, broker_cash, required_cash)
        if broker_cash is not None and broker_cash < required_cash:
            resized_qty = int(broker_cash // (float(price) * safety_buffer)) if price > 0 else 0
            if resized_qty >= 1:
                logger.warning("[US_ORDER][BROKER_CASH_RESIZE] env=%s symbol=%s old_qty=%s new_qty=%s broker_orderable_cash=%.2f", account_env, symbol, qty, resized_qty, broker_cash)
                qty = resized_qty
                intent = {**intent, "qty": qty, "notional_usd": qty * price}
                intent.setdefault("meta", {})
                if isinstance(intent.get("meta"), dict):
                    intent["meta"].update({"broker_orderable_cash_usd": broker_cash, "broker_cash_resized": True, "account_env": account_env})
            else:
                _CASH_EXHAUSTED_TICKS.add(tick_key)
                if order_key:
                    mark_order_intent_blocked(order_key, reason="broker_orderable_cash_insufficient")
                logger.error("[US_ORDER][BUY_BLOCKED] env=%s symbol=%s reason=broker_orderable_cash_insufficient broker_orderable_cash=%.2f required_cash=%.2f cash_exhausted=1", account_env, symbol, broker_cash, required_cash)
                return {"status": "BLOCKED", "reason": "broker_orderable_cash_insufficient", "cash_exhausted": True, "broker_orderable_cash": broker_cash, "required_cash": required_cash, "symbol": symbol, "side": side, "qty": qty, "intent": intent}

    # SELL 직전 broker balance hard guard: DB/cache가 아닌 KIS 최신 잔고 기준
    if side == "SELL":
        broker_pos = _get_broker_position(kis_client, symbol)
        broker_holding_qty = int(broker_pos.get("qty") or broker_pos.get("holding_qty") or 0) if broker_pos else 0
        broker_orderable_qty = int(broker_pos.get("orderable_qty") or 0) if broker_pos else 0
        if broker_pos is None:
            logger.warning("[US_ORDER][BROKER_QTY_CHECK][SKIP] env=%s symbol=%s reason=broker_balance_method_missing", account_env, symbol)
        else:
            logger.info("[US_ORDER][BROKER_QTY_CHECK] env=%s symbol=%s exchange=%s holding_qty=%s orderable_qty=%s currency=%s account_env=%s", account_env, symbol, exchange, broker_holding_qty, broker_orderable_qty, broker_pos.get("currency", "USD") if broker_pos else "USD", account_env)
        if broker_pos is not None and broker_orderable_qty <= 0:
            try:
                from trader.us.db.repos import find_recent_sell_ack
                recent_ack = find_recent_sell_ack(symbol=symbol, trade_date=trade_date)
            except Exception:
                recent_ack = None
            if recent_ack:
                return {"status": "OK_EXIT_POSITION_CLOSED", "reason": "broker_orderable_qty_zero_after_recent_sell_ack", "symbol": symbol, "side": side, "requires_reconcile": False, "reconciled": True, "intent": intent, "recent_sell_ack": recent_ack}
            if order_key:
                mark_order_intent_blocked(order_key, reason="broker_orderable_qty_zero")
            return {"status": "BLOCKED", "reason": "broker_orderable_qty_zero", "symbol": symbol, "side": side, "qty": qty, "intent": intent, "broker_position": broker_pos}
        if broker_pos and broker_orderable_qty < qty:
            qty = broker_orderable_qty
            intent = {**intent, "qty": qty, "notional_usd": qty * price}
        if broker_pos is None and int(intent.get("orderable_qty") or 0) > 0:
            logger.warning("[US_ORDER][BROKER_QTY_CHECK][INTENT_FALLBACK] env=%s symbol=%s reason=broker_balance_unavailable", account_env, symbol)
        from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

        # intent 또는 us_positions에서 holding_qty/orderable_qty 확보
        _pos_for_guard = {
            "holding_qty": intent.get("holding_qty")
                           or intent.get("available_qty")
                           or (intent.get("meta") or {}).get("holding_qty")
                           or broker_holding_qty,
            "orderable_qty": intent.get("orderable_qty")
                             or (intent.get("meta") or {}).get("orderable_qty")
                             or broker_orderable_qty,
            "sellable_qty": intent.get("sellable_qty")
                            or (intent.get("meta") or {}).get("sellable_qty")
                            or broker_orderable_qty,
            "position_source": "kis_broker_balance",
        }
        # DB fallback: intent에 orderable_qty가 없으면 us_positions 조회
        if not _pos_for_guard["orderable_qty"] and symbol:
            try:
                from trader.us.db.repos import load_us_positions_by_symbols
                _db_positions = load_us_positions_by_symbols([symbol])
                _db_pos = _db_positions.get(symbol, {})
                if _db_pos:
                    _pos_for_guard["holding_qty"] = _pos_for_guard["holding_qty"] or _db_pos.get("holding_qty") or _db_pos.get("qty")
                    _pos_for_guard["orderable_qty"] = _db_pos.get("orderable_qty") or _db_pos.get("qty")
                    _pos_for_guard["sellable_qty"] = _db_pos.get("sellable_qty") or _pos_for_guard["orderable_qty"]
                    _pos_for_guard["avg_cost"] = _db_pos.get("avg_cost") or _db_pos.get("entry_price") or _db_pos.get("avg_price")
                    _pos_for_guard["position_source"] = _db_pos.get("entry_price_source") or _db_pos.get("balance_source") or "us_positions"
            except Exception as _db_exc:
                logger.warning("[US_ORDER][BALANCE_MATCH][WARN] db fallback failed: %s", _db_exc)

        sell_qty, guard_meta = resolve_sell_qty(intent, _pos_for_guard)
        if not broker_pos and sell_qty <= 0 and int(intent.get("orderable_qty") or 0) > 0:
            sell_qty = min(qty, int(intent.get("orderable_qty") or qty))
            guard_meta = {**guard_meta, "holding_qty": intent.get("available_qty") or guard_meta.get("holding_qty"), "orderable_qty": intent.get("orderable_qty") or guard_meta.get("orderable_qty"), "sell_qty": sell_qty, "reason": "broker_check_unavailable_intent_fallback"}
        pending_sell_qty = _pending_sell_qty_for_symbol(symbol, trade_date) if broker_pos is not None else 0
        available_to_sell = max(0, int(guard_meta.get("orderable_qty") or guard_meta.get("holding_qty") or sell_qty or 0) - pending_sell_qty)
        if pending_sell_qty > 0 and available_to_sell < sell_qty:
            logger.warning(
                "[US_ORDER][SELL_AVAILABLE_TO_SELL_CLAMP] symbol=%s sell_qty=%d pending_sell_qty=%d available_to_sell=%d",
                symbol, sell_qty, pending_sell_qty, available_to_sell,
            )
            sell_qty = available_to_sell
            intent = {**intent, "qty": sell_qty, "notional_usd": sell_qty * price}
            intent.setdefault("meta", {})
            if isinstance(intent.get("meta"), dict):
                intent["meta"].update({"pending_sell_qty": pending_sell_qty, "available_to_sell": available_to_sell})

        logger.info(
            "[US_ORDER][BALANCE_MATCH] symbol=%s intent_qty=%s holding_qty=%s"
            " orderable_qty=%s sell_qty=%s",
            symbol,
            qty,
            guard_meta.get("holding_qty"),
            guard_meta.get("orderable_qty"),
            sell_qty,
        )

        if sell_qty <= 0:
            try:
                from trader.us.db.repos import find_recent_sell_ack
                recent_ack = find_recent_sell_ack(symbol=symbol, trade_date=trade_date)
            except Exception as ack_exc:
                logger.warning("[US_ORDER][SELL_NO_ORDERABLE][RECENT_ACK_WARN] symbol=%s err=%s", symbol, ack_exc)
                recent_ack = None
            if recent_ack:
                logger.warning(
                    "[US_ORDER][SELL_NO_ORDERABLE][RECENT_ACK_CLOSED] symbol=%s order_no=%s reason=no_orderable_qty",
                    symbol, recent_ack.get("order_no"),
                )
                return {
                    "status": "OK_EXIT_POSITION_CLOSED",
                    "reason": "no_orderable_qty_after_recent_sell_ack",
                    "symbol": symbol,
                    "side": side,
                    "requires_reconcile": False,
                    "reconciled": True,
                    "intent": intent,
                    "recent_sell_ack": recent_ack,
                }
            logger.error(
                "[US_ORDER][SELL_BLOCKED] symbol=%s reason=no_orderable_qty"
                " holding_qty=%s orderable_qty=%s",
                symbol,
                guard_meta.get("holding_qty"),
                guard_meta.get("orderable_qty"),
            )
            if order_key:
                mark_order_intent_blocked(order_key, reason="no_orderable_qty")
            return {
                "status": "BLOCKED",
                "reason": "no_orderable_qty",
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "intent": intent,
            }

        if sell_qty < qty:
            logger.warning(
                "[US_ORDER][SELL_QTY_CLAMP] symbol=%s old_qty=%d new_qty=%d"
                " holding_qty=%s orderable_qty=%s",
                symbol,
                qty,
                sell_qty,
                guard_meta.get("holding_qty"),
                guard_meta.get("orderable_qty"),
            )
            qty = sell_qty
            intent = {**intent, "qty": sell_qty, "notional_usd": sell_qty * price}
            intent.setdefault("meta", {})
            if isinstance(intent.get("meta"), dict):
                intent["meta"]["sell_qty_clamped"] = True

    if side == "SELL":
        intent.setdefault("meta", {})
        if isinstance(intent.get("meta"), dict):
            meta = intent["meta"]
            cost_basis = (
                intent.get("avg_cost")
                or intent.get("entry_price")
                or intent.get("avg_price")
                or meta.get("entry_price")
                or meta.get("avg_cost")
                or meta.get("avg_price")
                or (_pos_for_guard.get("avg_cost") if "_pos_for_guard" in locals() else None)
            )
            try:
                cost_basis_float = float(cost_basis) if cost_basis not in (None, "") else 0.0
            except (TypeError, ValueError):
                cost_basis_float = 0.0
            if cost_basis_float > 0:
                meta.setdefault("cost_basis_price_usd", cost_basis_float)
                meta.setdefault("cost_basis_source", "pre_sell_position_snapshot")
                meta.setdefault("pre_sell_avg_cost", cost_basis_float)
                pre_sell_qty = (
                    intent.get("holding_qty")
                    or intent.get("available_qty")
                    or meta.get("holding_qty")
                    or (_pos_for_guard.get("holding_qty") if "_pos_for_guard" in locals() else None)
                )
                if pre_sell_qty not in (None, ""):
                    meta.setdefault("pre_sell_qty", pre_sell_qty)
                meta.setdefault(
                    "pre_sell_position_source",
                    (_pos_for_guard.get("position_source") if "_pos_for_guard" in locals() else None) or "pre_sell_position_snapshot",
                )

    # Last possible fence and durable write occur immediately before the API.
    if context is not None and not context.broker_submit_allowed():
        from trader.us.execution.order_journal import append_order_event
        append_order_event("ORDER_FENCED", intent, context=context, broker_status="ORDER_FENCED_BEFORE_BROKER_SUBMIT")
        return {"status": "ORDER_FENCED_BEFORE_BROKER_SUBMIT", "reason": "stale_cancelled_or_superseded_tick",
                "broker_submit": False, "retry_order": False, "requires_reconcile": False, "intent": intent}
    from trader.us.execution.order_journal import append_order_event
    try:
        append_order_event("BROKER_SUBMIT_STARTED", intent, context=context)
    except Exception as exc:
        logger.critical("[US_ORDER][JOURNAL_FAILED] broker_submit=blocked error=%s", exc)
        return {"status": "ORDER_DISABLED_DURABLE_LEDGER_UNAVAILABLE", "reason": "durable_journal_write_failed", "broker_submit": False, "retry_order": False, "intent": intent}
    logger.info("[US_ORDER][SUBMIT] symbol=%s side=%s qty=%s price=%.4f", symbol, side, qty, price)
    logger.info(
        "[US_ORDER_SUBMIT_ATTEMPT] symbol=%s side=%s qty=%s limit_price=%.4f notional=%.4f",
        symbol,
        side,
        qty,
        price,
        float(intent.get("notional_usd") or (float(qty) * float(price) if price else 0.0)),
    )

    # ── KIS 주문 호출 (KIS ACK) ───────────────────────────────────────────
    # 중요: KIS ACK과 DB ACK을 반드시 분리한다.
    # KIS 주문 성공 후 DB 저장 실패는 REJECT가 아니라 ACK_DB_FAILED이다.
    order_no: str | None = None
    resp: Any = None
    try:
        if side == "BUY":
            resp = kis_client.place_us_buy_order(symbol, exchange, qty, price)
        else:
            resp = kis_client.place_us_sell_order(symbol, exchange, qty, price)
    except Exception as exc:
        # KIS API 자체 실패 → REJECT (SELL no-balance after ACK is reconciliatory, not fatal)
        msg = str(exc)
        if side == "SELL" and (is_no_balance_sell_reject(msg) or "잔고내역" in msg):
            try:
                from trader.us.db.repos import load_us_position_risk_state, save_us_position_risk_state
                st = load_us_position_risk_state(symbol, trade_date or "")
                st.update({"stale_broker_mismatch": True, "broker_position_mismatch": True, "sell_blocked_for_day": True, "reason": "broker_position_mismatch"})
                save_us_position_risk_state(symbol, trade_date or "", st)
            except Exception as stale_exc:
                logger.warning("[US_ORDER][BROKER_POSITION_MISMATCH][WARN] symbol=%s err=%s", symbol, stale_exc)
            try:
                from trader.us.db.repos import find_recent_sell_ack, load_us_positions_by_symbols
                recent_ack = find_recent_sell_ack(symbol=symbol, trade_date=trade_date)
                positions = load_us_positions_by_symbols([symbol]) if symbol else {}
                pos = positions.get(symbol, {}) if isinstance(positions, dict) else {}
                qty_now = int(pos.get("qty") or pos.get("holding_qty") or pos.get("orderable_qty") or 0) if pos else 0
                if recent_ack and qty_now <= 0:
                    logger.warning("[US_ORDER][SELL_NO_BALANCE][CLOSED] symbol=%s reason=%s", symbol, msg)
                    return {"status": "OK_EXIT_POSITION_CLOSED", "reason": "no_balance_after_recent_sell_ack", "symbol": symbol, "side": side, "requires_reconcile": False, "reconciled": True, "intent": intent}
                if recent_ack:
                    logger.warning("[US_ORDER][SELL_NO_BALANCE][RECONCILE_PENDING] symbol=%s reason=%s", symbol, msg)
                    return {"status": "WARN_SELL_REJECT_RECONCILE_PENDING", "reason": msg, "symbol": symbol, "side": side, "requires_reconcile": True, "intent": intent}
            except Exception as nb_exc:
                logger.warning("[US_ORDER][SELL_NO_BALANCE][WARN] reconcile probe failed: %s", nb_exc)
        deterministic = is_no_balance_sell_reject(msg) or _is_cash_insufficient_reject(msg) or any(token in msg.lower() for token in ("invalid quantity", "invalid price", "업무 거절", "주문 불가"))
        if not deterministic:
            append_order_event("BROKER_SUBMIT_RESULT_UNKNOWN", intent, context=context, broker_status="AMBIGUOUS_ACK", raw_response={"error": msg})
            return {"status": "BROKER_SUBMIT_RESULT_UNKNOWN", "reason": msg, "kis_ack": False,
                    "broker_submit": True, "retry_order": False, "requires_reconcile": True,
                    "submit_attempt_id": intent["submit_attempt_id"], "intent": intent}
        if side == "BUY" and _is_cash_insufficient_reject(msg):
            _CASH_EXHAUSTED_TICKS.add(_cash_tick_key(trade_date, intent))
            logger.error("[US_ORDER][CASH_EXHAUSTED] env=%s symbol=%s reason=broker_orderable_cash_insufficient cash_exhausted=1", account_env, symbol)
        logger.error("[US_ORDER][REJECT] env=%s symbol=%s side=%s error=%s", account_env, symbol, side, exc)
        reject_result = {
            "client_order_key": order_key,
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "qty": qty,
            "reason": msg,
        }
        _persist_with_trade_date(save_order_reject, reject_result)
        append_order_event("ORDER_REJECTED", intent, context=context, broker_status="REJECTED", raw_response={"reason": msg})
        if order_key:
            mark_order_intent_rejected(order_key, reason=msg)
        return {
            "status": "REJECT",
            "reason": msg,
            "kis_ack": False,
            "ack_db_saved": False,
            "requires_reconcile": False,
            "cash_exhausted": bool(side == "BUY" and _is_cash_insufficient_reject(msg)),
            "intent": intent,
        }

    # A broker response means submission occurred. Parsing/audit failures are
    # unresolved ACK states and must never enter broker rejection handling.
    try:
        from trader.us.execution.kis_us_response_parser import extract_order_no
        order_no = extract_order_no(resp)
    except Exception as exc:
        logger.error("[US_ORDER][ACK_PARSE_FAILED] symbol=%s err=%s", symbol, exc)
        append_order_event("BROKER_SUBMIT_RESULT_UNKNOWN", intent, context=context, broker_status="AMBIGUOUS_ACK", raw_response=resp)
        return {"status": "BROKER_SUBMIT_RESULT_UNKNOWN", "kis_ack": True, "broker_submit": True,
                "retry_order": False, "requires_reconcile": True, "raw_response": resp, "intent": intent}
    if not order_no:
        append_order_event("BROKER_SUBMIT_RESULT_UNKNOWN", intent, context=context, broker_status="AMBIGUOUS_ACK", raw_response=resp)
        return {"status": "BROKER_SUBMIT_RESULT_UNKNOWN", "kis_ack": True, "broker_submit": True,
                "retry_order": False, "requires_reconcile": True, "raw_response": resp, "intent": intent}
    try:
        append_order_event("BROKER_ACK_RECEIVED", intent, context=context, broker_order_no=order_no,
                           broker_status="ACK", raw_response=resp)
    except Exception as exc:
        logger.critical("[US_ORDER][ACK_JOURNAL_FAILED] symbol=%s order_no=%s err=%s", symbol, order_no, exc)
        return {"status": "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "kis_ack": True,
                "broker_submit": True, "retry_order": False, "requires_reconcile": True,
                "order_no": order_no, "intent": intent}
    logger.info("[US_ORDER][KIS_ACK] symbol=%s side=%s order_no=%s", symbol, side, order_no)
    logger.info("[US_ORDER_ACK] symbol=%s side=%s qty=%s order_no=%s", symbol, side, qty, order_no)


    # ── DB ACK 저장 (KIS 성공 이후 별도 try) ─────────────────────────────
    # KIS 주문이 성공했으므로 어떤 경우에도 REJECT로 기록하면 안 된다.
    from trader.us.utils.order_no import normalize_us_order_no
    ack_meta = {**(intent.get("meta") if isinstance(intent.get("meta"), dict) else {}), "raw_response": resp,
                "order_no_raw": str(order_no), "order_no_norm": normalize_us_order_no(order_no)}
    if side.upper() == "SELL":
        pre_qty = (
            ack_meta.get("pre_order_position_qty")
            or ack_meta.get("pre_sell_qty")
            or ack_meta.get("position_snapshot_qty")
            or ack_meta.get("holding_qty")
            or intent.get("holding_qty")
            or intent.get("available_qty")
        )
        if pre_qty not in (None, ""):
            try:
                ack_meta["pre_order_position_qty"] = int(float(pre_qty))
                ack_meta["pre_order_position_source"] = "pre_sell_position_snapshot"
            except (TypeError, ValueError):
                logger.warning("[US_ORDER][ACK_META][PRE_QTY_INVALID] symbol=%s pre_qty=%s", symbol, pre_qty)
    ack_result = {
        "client_order_key": order_key,
        "symbol": symbol,
        "exchange": exchange,
        "side": side,
        "qty_requested": qty,
        "qty_filled": 0,
        "avg_price_usd": price or None,
        "order_no": order_no,
        "status": "ACK",
        "dry_run": False,
        "committed_notional_usd": float(qty) * float(price),
        "env": account_env,
        "meta": ack_meta,
    }

    ack_db_saved = False
    try:
        ack_db_saved = bool(_persist_with_trade_date(save_order_ack, ack_result))
        if ack_db_saved:
            logger.info("[US_ORDER][ACK_DB_SAVE][OK] symbol=%s order_no=%s", symbol, order_no)
    except Exception:
        logger.exception(
            "[US_ORDER][ACK_DB_SAVE][FAILED] symbol=%s side=%s order_no=%s",
            symbol, side, ack_result.get("order_no"),
        )
        ack_db_saved = False

    if ack_db_saved:
        try:
            append_order_event("DB_ACK_PERSISTED", intent, context=context, broker_order_no=order_no or "", broker_status="ACK")
        except Exception as exc:
            logger.critical("[US_ORDER][DB_ACK_JOURNAL_FAILED] order_no=%s err=%s", order_no, exc)
            return {"status": "DB_ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "symbol": symbol,
                    "side": side, "order_no": order_no, "kis_ack": True, "ack_db_saved": True,
                    "broker_submit": True, "retry_order": False, "requires_reconcile": True, "intent": intent}

    if not ack_db_saved:
        logger.error(
            "[US_ORDER][ACK_DB_SAVE][FAILED_RETURN] symbol=%s side=%s order_no=%s",
            symbol, side, ack_result.get("order_no"),
        )
        return {
            "status": "ACK_DB_FAILED",
            "reason": "ack_db_save_failed",
            "symbol": symbol,
            "side": side,
            "ack": ack_result,
            "committed_notional_usd": ack_result.get("committed_notional_usd"),
            "requires_reconcile": True,
            "intent": intent,
            "kis_ack": True,
            "broker_submit": True,
            "ack_db_saved": False,
            "retry_order": False,
        }

    # ── intent 상태 업데이트 ───────────────────────────────────────────────
    try:
        if order_key:
            mark_order_intent_sent(order_key)
            _SENT_ORDER_KEYS.add(order_key)
    except Exception as mark_exc:
        logger.error(
            "[US_ORDER][INTENT_MARK_SENT_FAILED] symbol=%s order_no=%s error=%s",
            symbol, order_no, mark_exc,
        )

    # ── 최종 반환 ──────────────────────────────────────────────────────────
    if ack_db_saved:
        logger.info("[US_ORDER][ACK] symbol=%s order_no=%s", symbol, order_no)
        return {
            "status": "ACK",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "order_no": order_no,
            "response": resp,
            "intent": intent,
            "kis_ack": True,
            "ack_db_saved": True,
            "requires_reconcile": False,
        }
    else:
        logger.error(
            "[US_ORDER][RECONCILE_REQUIRED] symbol=%s order_no=%s "
            "reason=ack_db_failed_after_kis_success",
            symbol, order_no,
        )
        return {
            "status": _StatusCompat("ACK_DB_FAILED_RECONCILE_REQUIRED", "ACK_DB_FAILED"),
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "order_no": order_no,
            "response": resp,
            "intent": intent,
            "kis_ack": True,
            "ack_db_saved": False,
            "broker_submit": True,
            "retry_order": False,
            "requires_reconcile": True,
        }


def clear_sent_order_keys() -> None:
    """테스트 cleanup 용. in-memory주문 스토어도 함께 초기화."""
    _SENT_ORDER_KEYS.clear()
    try:
        from trader.us.db.repos import reset_memory_stores
        reset_memory_stores()
    except Exception:
        pass
