# -*- coding: utf-8 -*-
"""거래 메인 루프.

기존 trader.py의 설정/유틸을 분리하고, 메인 진입점만 남겨 전략 추가가
쉬운 구조로 변경했다.
"""
from __future__ import annotations

import json
import re
import time
import os
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Dict, List, Tuple, TYPE_CHECKING


try:
    from .config import (
        DAILY_CAPITAL,
        FAST_STOP,
        FORCE_SELL_PASSES_CLOSE,
        FORCE_SELL_PASSES_CUTOFF,
        FORCE_SELL_BLOCKED_LOTS,
        ALLOW_WHEN_CLOSED,
        ALLOW_PYRAMID,
        BASE_QTY_MODE,
        KST,
        LOG_DIR,
        RATE_SLEEP_SEC,
        EMERGENCY_GLOBAL_SELL,
        SELL_ALL_BALANCES_AT_CUTOFF,
        SELL_FORCE_TIME,
        SLIPPAGE_ENTER_GUARD_PCT,
        STATE_PATH,
        STRATEGY_REDUCTION_PRIORITY,
        USE_PULLBACK_ENTRY,
        PULLBACK_MAX_BUYS_PER_DAY,
        NEUTRAL_ENTRY_SCALE,
        MANUAL_HARD_STOP_LOSS_PCT,
        MANUAL_TRAILING_STOP_PCT,
        MANUAL_MAX_HOLDING_DAYS,
        _cfg,
        logger,
    )
except ImportError:
    # ALLOW_WHEN_CLOSED가 누락돼도 러너가 즉시 중단되지 않도록 안전한 기본값을 제공한다.
    from .config import (
        DAILY_CAPITAL,
        FAST_STOP,
        FORCE_SELL_PASSES_CLOSE,
        FORCE_SELL_PASSES_CUTOFF,
        FORCE_SELL_BLOCKED_LOTS,
        KST,
        LOG_DIR,
        BASE_QTY_MODE,
        RATE_SLEEP_SEC,
        EMERGENCY_GLOBAL_SELL,
        SELL_ALL_BALANCES_AT_CUTOFF,
        SELL_FORCE_TIME,
        SLIPPAGE_ENTER_GUARD_PCT,
        STATE_PATH,
        STRATEGY_REDUCTION_PRIORITY,
        USE_PULLBACK_ENTRY,
        PULLBACK_MAX_BUYS_PER_DAY,
        NEUTRAL_ENTRY_SCALE,
        MANUAL_HARD_STOP_LOSS_PCT,
        MANUAL_TRAILING_STOP_PCT,
        MANUAL_MAX_HOLDING_DAYS,
        _cfg,
        logger,
    )

    ALLOW_WHEN_CLOSED = False
    logger.warning("[CONFIG] ALLOW_WHEN_CLOSED missing; defaulting to False")
    ALLOW_PYRAMID = False
    logger.warning("[CONFIG] ALLOW_PYRAMID missing; defaulting to False")
from . import signals
from trader.time_utils import MARKET_CLOSE, MARKET_OPEN, is_trading_day
from trader.subject_flow import get_subject_flow_with_fallback, reset_flow_call_count
from trader.execution import record_entry_state
from trader.strategy_rules import strategy_entry_gate, strategy_trigger_label
from trader.exit_allocation import allocate_sell_qty, apply_sell_allocation
from trader.code_utils import normalize_code
from trader.ledger import (
    record_buy_fill,
    remaining_qty_for_strategy,
    reconcile_with_broker_holdings,
    strategy_avg_price,
)
from trader.ctx_schema import normalize_daily_ctx, normalize_intraday_ctx
from trader import state_store as runtime_state_store
from trader.lot_state_store import load_lot_state, save_lot_state
from trader.position_state_store import (
    load_position_state,
    reconcile_with_broker,
    reconcile_positions,
    save_position_state,
)
from .core import *  # noqa: F401,F403 - 전략 유틸 전체 노출로 확장성 확보

if TYPE_CHECKING:
    # core 쪽에 구현돼 있는 헬퍼들을 타입체커에게만 명시적으로 알려준다.
    from .core import (
        _this_iso_week_key,
        _get_effective_ord_cash,
        _to_float,
        _to_int,
        _weight_to_qty,
        _classify_champion_grade,
        _update_market_regime,
        _notional_to_qty,
        _fetch_balances,
        _init_position_state_from_balance,
        _sell_once,
        _adaptive_exit,
        _compute_daily_entry_context,
        _compute_intraday_entry_context,
        _safe_get_price,
        _round_to_tick,
        _init_position_state,
        _detect_pullback_reversal,
        _has_bullish_trend_structure,
    )


def main(
    capital_override: float | None = None,
    selected_stocks: list[dict[str, Any]] | None = None,
):
    reset_flow_call_count()
    effective_capital = (
        int(capital_override) if capital_override is not None else DAILY_CAPITAL
    )
    kis = KisAPI()
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    rebalance_date = get_rebalance_anchor_date()
    logger.info(
        f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date} (anchor={REBALANCE_ANCHOR}, ref={WEEKLY_ANCHOR_REF})"
    )
    logger.info(
        f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
        f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}"
    )
    logger.info(
        f"[💰 CAPITAL] {effective_capital:,}원 (configured DAILY_CAPITAL={DAILY_CAPITAL:,})"
    )
    logger.info(f"[🛡️ SLIPPAGE_ENTER_GUARD_PCT] {SLIPPAGE_ENTER_GUARD_PCT:.2f}%")

    # 상태 복구
    state_loaded_at = datetime.now(KST)
    state_loaded_date = state_loaded_at.strftime("%Y-%m-%d")
    state_loaded_str = state_loaded_at.strftime("%Y-%m-%d %H:%M:%S")
    state_loaded_midnight = f"{state_loaded_date} 00:00:00"

    holding, traded = load_state()
    lot_state_path = "bot_state/state.json"
    lot_state = load_lot_state(lot_state_path)
    position_state_path = str(STATE_PATH)
    position_state = load_position_state(position_state_path)
    position_state_dirty = False
    runtime_state = runtime_state_store.load_state()
    triggered_today: set[str] = set()
    s1_done_today: set[tuple[str, str]] = set()
    last_today_prefix: str | None = None

    if isinstance(traded, (set, list, tuple)):
        logger.warning(
            f"[STATE-MIGRATE] traded 타입 {type(traded)} → dict로 마이그레이션(중복 진입 가드 유지)"
        )
        traded = {
            code: {"buy_time": state_loaded_midnight, "qty": 0, "price": 0.0}
            for code in traded
        }
    elif not isinstance(traded, dict):
        logger.warning(
            f"[STATE-FORMAT] traded 타입 {type(traded)} 지원 안 함 → 빈 dict로 재설정"
        )
        traded = {}
    if isinstance(traded, dict):
        traded = {normalize_code(k): v for k, v in traded.items() if normalize_code(k)}
    if isinstance(holding, dict):
        holding = {normalize_code(k): v for k, v in holding.items() if normalize_code(k)}

    def _traded_codes(traded_state: Any) -> List[str]:
        if isinstance(traded_state, dict):
            return list(traded_state.keys())
        return []

    def _traded_today(traded_state: Any, today_prefix: str) -> set:
        if not isinstance(traded_state, dict):
            return set()

        today_codes = set()
        for code, payload in traded_state.items():
            payload = payload or {}
            buy_time = payload.get("buy_time")
            status = payload.get("status")
            # pending/other 상태는 재시도 허용, filled/기존(None)만 중복 방지
            if status not in (None, "filled"):
                continue
            if isinstance(buy_time, str) and buy_time.startswith(today_prefix):
                today_codes.add(code)
        return today_codes

    def _record_trade(traded_state: Any, code: str, payload: Dict[str, Any]) -> None:
        try:
            traded_state[normalize_code(code)] = payload
        except Exception:
            logger.warning(
                f"[TRADED-STATE] traded에 코드 추가 실패: type={type(traded_state)}"
            )

    def _load_trade_log(days: int = 7) -> List[Dict[str, Any]]:
        logs: List[Dict[str, Any]] = []
        today = datetime.now(KST).date()
        for offset in range(days):
            day = today - timedelta(days=offset)
            path = LOG_DIR / f"trades_{day.strftime('%Y-%m-%d')}.json"
            if not path.exists():
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            logs.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            except Exception:
                logger.exception("[TRADE_LOG] failed to read %s", path)
        return logs

    def _save_runtime_state() -> None:
        try:
            runtime_state_store.save_state(runtime_state)
        except Exception:
            logger.exception("[RUNTIME_STATE] save failed")

    def _save_position_state_now() -> None:
        nonlocal position_state_dirty
        if position_state_dirty:
            save_position_state(position_state_path, position_state)
            position_state_dirty = False

    def _lot_state_signature(state: dict) -> tuple:
        lots = state.get("lots")
        if not isinstance(lots, list):
            return tuple()
        return tuple(
            (
                str(lot.get("lot_id")),
                str(lot.get("pdno")),
                int(lot.get("remaining_qty") or 0),
            )
            for lot in lots
        )

    def _maybe_save_lot_state(before_signature: tuple) -> None:
        after_signature = _lot_state_signature(lot_state)
        if after_signature != before_signature:
            save_lot_state(lot_state_path, lot_state)

    def _ensure_position_entry(
        code: str, strategy_id: int | str
    ) -> Dict[str, Any]:
        code_key = normalize_code(code)
        sid_key = str(strategy_id)
        pos = position_state.setdefault("positions", {}).setdefault(
            code_key,
            {
                "strategies": {},
            },
        )
        entries = pos.setdefault("strategies", {})
        entry = entries.get(sid_key)
        if isinstance(entry, dict):
            return entry
        now_ts = datetime.now(KST).isoformat()
        entry = {
            "qty": int(remaining_qty_for_strategy(lot_state, code_key, sid_key)),
            "avg_price": float((holding.get(code) or {}).get("buy_price") or 0.0),
            "entry": {
                "time": now_ts,
                "strategy_id": sid_key,
                "engine": "unknown",
                "entry_reason": "RECONCILE",
                "order_type": "unknown",
                "best_k": None,
                "tgt_px": None,
                "gap_pct_at_entry": None,
            },
            "meta": {
                "pullback_peak_price": None,
                "pullback_reversal_price": None,
                "pullback_reason": None,
            },
            "flags": {
                "bear_s1_done": False,
                "bear_s2_done": False,
                "sold_p1": False,
                "sold_p2": False,
            },
        }
        entries[sid_key] = entry
        return entry

    def _set_position_flags(code: str, strategy_id: int | str, **flags: bool) -> None:
        nonlocal position_state_dirty
        pos = position_state.setdefault("positions", {}).setdefault(
            normalize_code(code),
            {
                "strategies": {},
            },
        )
        entries = pos.setdefault("strategies", {})
        entry = entries.setdefault(str(strategy_id), {})
        entry_flags = entry.setdefault(
            "flags",
            {"bear_s1_done": False, "bear_s2_done": False, "sold_p1": False, "sold_p2": False},
        )
        flags_before = dict(entry_flags)
        for key, value in flags.items():
            entry_flags[key] = bool(value)
        logger.info(
            "[FLAGS] code=%s flags_before=%s flags_after=%s",
            normalize_code(code),
            flags_before,
            entry_flags,
        )
        position_state_dirty = True

    def _update_last_price_memory(code: str, current_price: float, now_ts: str) -> None:
        nonlocal position_state_dirty
        memory = position_state.setdefault("memory", {})
        code_key = normalize_code(code)
        memory.setdefault("last_price", {})[code_key] = float(current_price)
        memory.setdefault("last_seen", {})[code_key] = now_ts
        position_state_dirty = True

    def _pullback_stop_hit(
        code: str, current_price: float, strategy_id: int | str = 5
    ) -> bool:
        pos = position_state.get("positions", {}).get(normalize_code(code))
        if not isinstance(pos, dict):
            return False
        entries = pos.get("strategies", {})
        entry = entries.get(str(strategy_id))
        if not isinstance(entry, dict):
            return False
        meta = entry.get("meta", {}) or {}
        reversal_price = meta.get("pullback_reversal_price")
        if reversal_price is None:
            return False
        try:
            return float(current_price) < float(reversal_price) * (1 - FAST_STOP)
        except Exception:
            return False

    def _manual_exit_intent(
        code_key: str,
        entry: Dict[str, Any],
        available_qty: int,
        avg_price: float,
        now_dt: datetime,
        rebalance_anchor: str,
    ) -> Tuple[str | None, int]:
        cur_price = _safe_get_price(kis, code_key)
        if cur_price is None or cur_price <= 0:
            return None, 0
        high_val = float(entry.get("high_watermark") or entry.get("meta", {}).get("high") or avg_price)
        high_val = max(high_val, float(cur_price))
        entry["high_watermark"] = high_val
        entry.setdefault("meta", {})["high"] = high_val
        entry["last_update_ts"] = now_dt.isoformat()

        pnl_pct = (float(cur_price) - float(avg_price)) / float(avg_price) * 100.0
        if MANUAL_HARD_STOP_LOSS_PCT and pnl_pct <= -abs(MANUAL_HARD_STOP_LOSS_PCT):
            return "manual_hard_stop", int(available_qty)

        trail_pct = abs(MANUAL_TRAILING_STOP_PCT or 0.0)
        if trail_pct and float(cur_price) <= high_val * (1 - trail_pct / 100.0):
            return "manual_trailing_stop", int(available_qty)

        if MANUAL_MAX_HOLDING_DAYS:
            try:
                entry_ts = entry.get("entry_ts") or entry.get("entry", {}).get("time")
                entry_dt = datetime.fromisoformat(entry_ts) if entry_ts else None
            except Exception:
                entry_dt = None
            if entry_dt:
                if (now_dt.date() - entry_dt.date()).days >= MANUAL_MAX_HOLDING_DAYS:
                    return "manual_time_cut", int(available_qty)
                try:
                    rebalance_dt = date.fromisoformat(rebalance_anchor)
                    if entry_dt.date() < rebalance_dt:
                        return "manual_rebalance_cut", int(available_qty)
                except Exception:
                    pass

        return None, 0

    def _build_exit_intents(code: str, regime_mode: str) -> list[dict[str, Any]]:
        nonlocal position_state_dirty
        intents: list[dict[str, Any]] = []
        code_key = normalize_code(code)
        pos_state = position_state.get("positions", {}).get(code_key)
        if not isinstance(pos_state, dict):
            return intents
        strategies = pos_state.get("strategies", {})
        if not isinstance(strategies, dict):
            return intents
        for sid, entry in strategies.items():
            if not isinstance(entry, dict):
                continue
            available_qty = remaining_qty_for_strategy(lot_state, code_key, sid)
            if available_qty <= 0:
                continue
            avg_price = strategy_avg_price(lot_state, code_key, sid)
            if avg_price is None:
                continue
            flags = entry.get("flags", {}) or {}
            meta = entry.get("meta", {}) or {}
            high_value = float(meta.get("high") or 0.0)
            if not high_value or high_value <= 0:
                high_value = float(avg_price)
            high_value = max(high_value, float(avg_price))
            meta["high"] = high_value
            entry["high_watermark"] = max(float(entry.get("high_watermark") or 0.0), high_value)
            entry["last_update_ts"] = datetime.now(KST).isoformat()
            pos_view = {
                "qty": int(available_qty),
                "buy_price": float(avg_price),
                "high": high_value,
                "sold_p1": bool(flags.get("sold_p1", False)),
                "sold_p2": bool(flags.get("sold_p2", False)),
                "name": entry.get("entry", {}).get("name"),
                "k_value": entry.get("entry", {}).get("best_k"),
                "target_price_src": entry.get("entry", {}).get("tgt_px"),
            }
            if str(sid) in {"MANUAL", "LEGACY"}:
                reason, sell_qty = _manual_exit_intent(
                    code_key,
                    entry,
                    int(available_qty),
                    float(avg_price),
                    datetime.now(KST),
                    str(rebalance_date),
                )
            else:
                reason, sell_qty = _adaptive_exit(
                    kis,
                    code_key,
                    pos_view,
                    regime_mode=regime_mode,
                )
            if sell_qty:
                intents.append(
                    {
                        "code": code_key,
                        "strategy_id": sid,
                        "sell_qty": int(sell_qty),
                        "reason": reason or "adaptive_exit",
                    }
                )
            meta["high"] = float(pos_view.get("high") or meta.get("high") or 0.0)
            entry["high_watermark"] = max(
                float(entry.get("high_watermark") or 0.0), float(meta.get("high") or 0.0)
            )
            entry["meta"] = meta
            flags["sold_p1"] = bool(pos_view.get("sold_p1", flags.get("sold_p1")))
            flags["sold_p2"] = bool(pos_view.get("sold_p2", flags.get("sold_p2")))
            entry["flags"] = flags
            position_state_dirty = True
        return intents

    def _remaining_qty_for(pdno: str) -> int:
        return sum(
            int(lot.get("remaining_qty") or 0)
            for lot in lot_state.get("lots", [])
            if normalize_code(lot.get("pdno")) == normalize_code(pdno)
        )

    def _ledger_total_available_qty(code: str) -> int:
        return sum(
            int(lot.get("remaining_qty") or 0)
            for lot in lot_state.get("lots", [])
            if normalize_code(lot.get("pdno")) == normalize_code(code)
        )

    def _cap_sell_qty(code: str, requested_qty: int) -> int:
        return min(int(requested_qty), int(_ledger_total_available_qty(code)))

    def _normalize_strategy_id(value: Any) -> int:
        try:
            strategy_num = int(value)
        except Exception:
            return 1
        if 1 <= strategy_num <= 5:
            return strategy_num
        return 1

    def _derive_strategy_id(payload: Dict[str, Any]) -> int:
        raw = (
            payload.get("strategy_id")
            or payload.get("strategyId")
            or payload.get("strategy_no")
            or payload.get("strategyNo")
        )
        if raw is not None and str(raw).isdigit():
            strategy_id = _normalize_strategy_id(raw)
            if strategy_id != int(raw):
                logger.info(
                    "[STRATEGY_ID_NORMALIZE] raw=%s -> %s (clamped)",
                    raw,
                    strategy_id,
                )
            return strategy_id
        name = str(payload.get("strategy") or "")
        name_lower = name.lower()
        match = re.search(r"(?:전략|strategy)\s*([1-5])", name_lower)
        if match:
            strategy_id = _normalize_strategy_id(match.group(1))
            logger.info(
                "[STRATEGY_ID_DERIVE] source=strategy_name(%s) -> %s",
                name,
                strategy_id,
            )
            return strategy_id
        if "pullback" in name_lower or "눌림목" in name:
            logger.info("[STRATEGY_ID_DERIVE] source=pullback -> 5")
            return 5
        logger.info("[STRATEGY_ID_DERIVE] source=default -> 1")
        return 1

    def _build_lot_id(result: Any, fallback_ts: str, pdno: str) -> str:
        order_no = ""
        fill_seq = ""
        if isinstance(result, dict):
            out = result.get("output") or {}
            order_no = (
                out.get("ODNO")
                or out.get("ord_no")
                or out.get("order_no")
                or result.get("ODNO")
                or result.get("ord_no")
                or result.get("order_no")
                or ""
            )
            fill_seq = (
                out.get("CCLD_SQ")
                or out.get("ccld_sq")
                or out.get("fill_seq")
                or out.get("CCLD_NO")
                or out.get("ccld_no")
                or ""
            )
        if not order_no:
            order_no = f"NOORDER-{normalize_code(pdno)}-{fallback_ts}"
        if not fill_seq:
            fill_seq = "0"
        return f"{kis.CANO}-{kis.ACNT_PRDT_CD}-{order_no}-{fill_seq}"

    def _estimate_sold_qty(
        code: str, requested_qty: int, prev_qty: int, delay_sec: float = 1.0
    ) -> int:
        if requested_qty <= 0:
            return 0
        try:
            time.sleep(delay_sec)
            try:
                balances = _fetch_balances(kis, ttl_sec=0)
            except TypeError:
                balances = _fetch_balances(kis)
        except Exception:
            return int(requested_qty)
        for row in balances:
            if normalize_code(row.get("code")) != normalize_code(code):
                continue
            new_qty = int(row.get("qty") or 0)
            sold = max(0, int(prev_qty) - int(new_qty))
            if sold <= 0:
                return 0
            return min(int(requested_qty), int(sold))
        return int(requested_qty)

    def _sync_position_state_qty(code: str) -> None:
        nonlocal position_state_dirty
        code_key = normalize_code(code)
        pos = position_state.get("positions", {}).get(code_key)
        if not isinstance(pos, dict):
            return
        strategies = pos.get("strategies", {})
        if not isinstance(strategies, dict):
            return
        for sid in list(strategies.keys()):
            remaining = remaining_qty_for_strategy(lot_state, code_key, sid)
            if remaining <= 0:
                strategies.pop(sid, None)
                continue
            entry = strategies.get(sid)
            if not isinstance(entry, dict):
                strategies.pop(sid, None)
                continue
            if int(entry.get("qty") or 0) > int(remaining):
                logger.warning(
                    "[STATE] qty exceeds ledger after sell: code=%s sid=%s state=%s ledger=%s",
                    code_key,
                    sid,
                    entry.get("qty"),
                    remaining,
                )
            entry["qty"] = int(remaining)
        if not strategies:
            position_state.get("positions", {}).pop(code_key, None)
        position_state_dirty = True

    def _apply_sell_to_ledger_with_balance(
        code: str,
        requested_qty: int,
        sell_ts: str,
        result: Any,
        scope: str = "strategy",
        trigger_strategy_id: int | None = None,
        prev_qty_before: int | None = None,
        allow_blocked: bool = False,
    ) -> None:
        if not _is_order_success(result):
            return
        if scope != "strategy" and not (SELL_ALL_BALANCES_AT_CUTOFF or EMERGENCY_GLOBAL_SELL):
            raise RuntimeError(
                f"[SELL-ALLOC] global scope used without force sell: code={code} scope={scope}"
            )
        prev_qty = int(
            prev_qty_before
            if prev_qty_before is not None
            else (holding.get(code) or {}).get("qty") or requested_qty
        )
        sold_qty = 0
        for delay_sec in (0.5, 1.0, 2.0):
            sold_qty = _estimate_sold_qty(
                code, requested_qty, prev_qty, delay_sec=delay_sec
            )
            if sold_qty > 0:
                break
        if sold_qty <= 0:
            logger.warning(
                "[SELL-ALLOC] sold_qty unresolved: code=%s requested=%s prev_qty=%s",
                normalize_code(code),
                requested_qty,
                prev_qty,
            )
            try:
                balances = _fetch_balances(kis, ttl_sec=0)
            except TypeError:
                balances = _fetch_balances(kis)
            reconcile_with_broker_holdings(lot_state, balances)
            return
        if scope == "strategy" and trigger_strategy_id is None:
            raise RuntimeError(
                f"[SELL-ALLOC] strategy scope requires trigger_strategy_id: code={code}"
            )
        if scope == "strategy":
            available_qty = remaining_qty_for_strategy(
                lot_state, code, trigger_strategy_id
            )
            if available_qty < int(requested_qty):
                raise RuntimeError(
                    "[SELL-ALLOC] insufficient strategy qty: code=%s sid=%s available=%s requested=%s"
                    % (normalize_code(code), trigger_strategy_id, available_qty, requested_qty)
                )
        before_lot_signature = _lot_state_signature(lot_state)
        before_qty_total = _ledger_total_available_qty(code)
        before_qty_strategy = (
            remaining_qty_for_strategy(lot_state, code, trigger_strategy_id)
            if scope == "strategy"
            else before_qty_total
        )
        allocations = allocate_sell_qty(
            lot_state,
            code,
            int(sold_qty),
            scope=scope,
            trigger_strategy_id=trigger_strategy_id,
        )
        if not allocations:
            if sold_qty > 0 and scope != "strategy":
                now_ts = datetime.now(KST).isoformat()
                lot_state.setdefault("lots", []).append(
                    {
                        "lot_id": f"{normalize_code(code)}-MANUAL-{now_ts}",
                        "pdno": normalize_code(code),
                        "strategy_id": "MANUAL",
                        "engine": "reconcile",
                        "entry_ts": now_ts,
                        "entry_price": 0.0,
                        "qty": int(sold_qty),
                        "remaining_qty": int(sold_qty),
                        "meta": {"reconciled": True, "manual": True},
                    }
                )
                allocations = [{"strategy_id": "MANUAL", "qty": int(sold_qty)}]
            else:
                raise RuntimeError(
                    f"[SELL-ALLOC] no allocations for strategy sell: code={code} sid={trigger_strategy_id}"
                )
        broker_qty_after = max(0, int(prev_qty) - int(sold_qty))
        logger.info(
            "[SELL-ALLOC] code=%s requested_qty=%s scope=%s allocations=%s sold_qty=%s broker_qty_before=%s broker_qty_after=%s",
            normalize_code(code),
            int(requested_qty),
            scope,
            allocations,
            int(sold_qty),
            int(prev_qty),
            int(broker_qty_after),
        )
        sold_total = apply_sell_allocation(
            lot_state,
            code,
            allocations,
            sell_ts,
            allow_blocked=allow_blocked,
        )
        after_qty_total = _ledger_total_available_qty(code)
        after_qty_strategy = (
            remaining_qty_for_strategy(lot_state, code, trigger_strategy_id)
            if scope == "strategy"
            else after_qty_total
        )
        expected_before = before_qty_strategy
        expected_after = max(0, int(expected_before) - int(sold_total))
        if after_qty_strategy != expected_after:
            resolved = False
            for delay_sec in (0.5, 1.0, 2.0):
                time.sleep(delay_sec)
                retry_sold = _estimate_sold_qty(code, requested_qty, prev_qty, delay_sec=0)
                retry_expected = max(0, int(expected_before) - int(retry_sold))
                if after_qty_strategy == retry_expected:
                    resolved = True
                    break
            if not resolved:
                logger.warning(
                    "[SELL-ALLOC] ledger mismatch: code=%s sid=%s before=%s sold=%s after=%s"
                    % (
                        normalize_code(code),
                        trigger_strategy_id,
                        expected_before,
                        sold_total,
                        after_qty_strategy,
                    )
                )
                try:
                    balances = _fetch_balances(kis, ttl_sec=0)
                except TypeError:
                    balances = _fetch_balances(kis)
                reconcile_with_broker_holdings(lot_state, balances)
        _maybe_save_lot_state(before_lot_signature)
        _sync_position_state_qty(code)
        _save_position_state_now()

    def _cleanup_expired_pending(
        traded_state: dict, now_dt: datetime, ttl_sec: int = 300
    ) -> set:
        expired: set[str] = set()
        if not isinstance(traded_state, dict):
            return expired

        for code, payload in list(traded_state.items()):
            payload = payload or {}
            if payload.get("status") != "pending":
                continue

            ts = payload.get("pending_since") or payload.get("buy_time")
            if not isinstance(ts, str):
                continue

            try:
                pending_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=now_dt.tzinfo
                )
                if (now_dt - pending_dt).total_seconds() > ttl_sec:
                    logger.warning(
                        f"[PENDING-EXPIRE] {code}: {ttl_sec}s 초과 → pending 제거"
                    )
                    traded_state.pop(code, None)
                    expired.add(code)
            except Exception:
                continue
        return expired

    guard_state_date: date | None = None
    guard_state: dict[str, Any] = {
        "period": "daily",
        "s1_target": {},
        "s1_nontarget": {},
        "s2_target": {},
        "s2_nontarget": {},
    }

    def _guard_state_file(day: date):
        return LOG_DIR / f"regime_guards_{day}.json"

    def _guard_state_template() -> dict[str, Any]:
        return {
            "period": "daily",
            "s1_target": {},
            "s1_nontarget": {},
            "s2_target": {},
            "s2_nontarget": {},
        }

    def _load_guard_state(day: date) -> dict[str, Any]:
        state = _guard_state_template()
        path = _guard_state_file(day)
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f) or {}
                for key in state.keys():
                    if key == "period":
                        state[key] = payload.get("period", "daily")
                    else:
                        state[key] = payload.get(key, {}) or {}
            except Exception as e:
                logger.warning(f"[REGIME-GUARD][LOAD] {day} 실패: {e}")
        return state

    def _persist_guard_state(day: date) -> None:
        try:
            path = _guard_state_file(day)
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"date": str(day), **guard_state}
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[REGIME-GUARD][SAVE] {day} 실패: {e}")

    def _ensure_guard_state(day: date) -> None:
        nonlocal guard_state_date, guard_state
        if guard_state_date != day:
            guard_state = _load_guard_state(day)
            guard_state_date = day

    def _guard_entry(
        bucket: str, day: date, code: str, strategy_id: int | str, base_qty: int
    ) -> dict:
        _ensure_guard_state(day)
        bucket_state = guard_state.setdefault(bucket, {})
        key = f"{normalize_code(code)}:{strategy_id}"
        entry = bucket_state.get(key)
        if entry is None:
            entry = {"base_qty": int(base_qty), "sold": 0}
            bucket_state[key] = entry
            _persist_guard_state(day)
        elif BASE_QTY_MODE == "current":
            base_int = int(base_qty)
            if entry.get("base_qty") != base_int:
                entry["base_qty"] = base_int
                _persist_guard_state(day)
        return entry

    def _s1_guard_target(
        today_date, code: str, strategy_id: int | str, base_qty: int
    ) -> dict:
        return _guard_entry("s1_target", today_date, code, strategy_id, base_qty)

    def _s1_guard_nontarget(
        today_date, code: str, strategy_id: int | str, base_qty: int
    ) -> dict:
        return _guard_entry("s1_nontarget", today_date, code, strategy_id, base_qty)

    def _s2_guard_target(
        today_date, code: str, strategy_id: int | str, base_qty: int
    ) -> dict:
        return _guard_entry("s2_target", today_date, code, strategy_id, base_qty)

    def _s2_guard_nontarget(
        today_date, code: str, strategy_id: int | str, base_qty: int
    ) -> dict:
        return _guard_entry("s2_nontarget", today_date, code, strategy_id, base_qty)

    def _sell_result_status(result: Any) -> tuple[str, str | None]:
        try:
            if isinstance(result, dict):
                if str(result.get("status")) == "SKIPPED":
                    return "SKIP", str(result.get("skip_reason") or "")
                if str(result.get("rt_cd")) == "0":
                    return "SENT", None
        except Exception:
            pass
        return "ERROR", None

    def _pending_block(
        traded_state: dict, code: str, now_dt: datetime, block_sec: int = 45
    ) -> bool:
        if not isinstance(traded_state, dict):
            return False
        payload = traded_state.get(code) or {}
        if payload.get("status") != "pending":
            return False

        ts = payload.get("pending_since") or payload.get("buy_time")
        if not isinstance(ts, str):
            return True

        try:
            pending_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=now_dt.tzinfo
            )
            return (now_dt - pending_dt).total_seconds() <= block_sec
        except Exception:
            return True

    def _is_balance_reflected(
        code: str, prev_qty: int = 0, delay_sec: float = 1.0
    ) -> bool:
        try:
            time.sleep(delay_sec)
            balances = _fetch_balances(kis, ttl_sec=0)
        except Exception as e:
            logger.warning(f"[BAL-REFRESH-FAIL] {code}: 잔고 확인 실패 {e}")
            return False

        for row in balances:
            try:
                if normalize_code(row.get("code")) != normalize_code(code):
                    continue
                qty_here = _to_int(row.get("qty") or 0)
                sellable_here = _to_int(
                    (row.get("sell_psbl_qty") or row.get("ord_psbl_qty")) or 0
                )
                baseline_qty = max(0, int(prev_qty))
                if qty_here > baseline_qty or sellable_here > baseline_qty:
                    return True
            except Exception:
                continue

        return False

    def _subject_flow_gate(
        code: str,
        info: Dict[str, Any],
        current_price: float,
        target_price: float | None,
        vwap_val: float | None,
    ) -> tuple[bool, Dict[str, Any], float]:
        day_turnover_krw = _to_float(
            info.get("prev_turnover")
            or info.get("avg_turnover")
            or info.get("turnover"),
            0.0,
        )
        market = (info.get("market") or "KOSDAQ").upper()
        flow = get_subject_flow_with_fallback(
            kis, code, market, float(day_turnover_krw or 0.0)
        )
        score = flow.get("score") or {}

        turnover_guard = float(CHAMPION_A_RULES.get("min_turnover") or 0.0)
        ob_guard = 0.0
        ob_strength_val: float = 0.0
        try:
            ob_strength_val = float(
                _to_float(kis.get_orderbook_strength(code), 0.0) or 0.0
            )
        except Exception as e:
            logger.warning(f"[OB_STRENGTH_FAIL] {code}: {e}")

        if flow.get("degraded"):
            turnover_guard *= float(flow.get("turnover_guard_mult") or 1.0)
            ob_guard += float(flow.get("ob_strength_add") or 0.0)

        ok = bool(flow.get("flow_ok"))
        reason_tag = None

        if turnover_guard > 0 and float(day_turnover_krw or 0.0) < turnover_guard:
            ok = False
            reason_tag = "LOW_TURNOVER"
        if ob_guard > 0 and ob_strength_val < ob_guard:
            ok = False
            reason_tag = "OB_WEAK"

        if not ok:
            if reason_tag is None:
                decision = str(flow.get("decision") or "")
                if decision.startswith("BLOCK"):
                    reason_tag = "SUBJECT_FLOW_FAIL_BLOCK"
                else:
                    reason_tag = "SUBJECT_FLOW_WEAK"

            logger.info(
                "[%s] code=%s market=%s last=%s target=%s vwap=%s turnover_krw=%.0f "
                "spread_ticks=%s orderbook_strength=%s smart_money_krw=%s smart_money_ratio=%.6f "
                "flow_used=%s flow_policy=%s degraded_mode=%s",
                reason_tag,
                code,
                market,
                current_price,
                target_price,
                vwap_val,
                float(day_turnover_krw or 0.0),
                None,
                ob_strength_val,
                score.get("smart_money_krw"),
                float(score.get("smart_money_ratio") or 0.0),
                flow.get("used"),
                flow.get("policy"),
                flow.get("degraded"),
            )

        return ok, flow, ob_strength_val

    def _is_order_success(res: Any) -> bool:
        if not isinstance(res, dict):
            return False
        rt_cd = str(res.get("rt_cd") or res.get("rtCode") or "").strip()
        return rt_cd in ("0", "0000", "OK")

    def _extract_fill_price(res: Any, fallback_price: float) -> float:
        if isinstance(res, dict):
            output = res.get("output") or {}
            for payload in (output, res):
                for key in (
                    # 체결가/평균가 후보
                    "ccld_prc",
                    "ccld_unpr",
                    "tot_ccld_unpr",
                    "tot_ccld_prc",
                    "avg_price",
                    "avg_prvs",
                    "fill_price",
                    # 주문가(후순위)
                    "prdt_price",
                    "ord_unpr",
                    "ord_prc",
                    "order_price",
                ):
                    val = None
                    if isinstance(payload, dict):
                        val = payload.get(key)
                    if val not in (None, ""):
                        try:
                            return float(val)
                        except Exception:
                            continue
        return float(fallback_price)

    logger.info(
        f"[상태복구] holding: {list(holding.keys())}, traded: {_traded_codes(traded)}"
    )

    pullback_buys_today = 0
    pullback_buy_date = datetime.now(KST).date()

    # === [NEW] 주간 리밸런싱 강제/중복 방지 ===
    targets: List[Dict[str, Any]] = []
    if selected_stocks is not None:
        targets = list(selected_stocks)
        logger.info(
            "[REBALANCE] injected selected_stocks count=%d (skip API fetch)",
            len(targets),
        )
    elif REBALANCE_ANCHOR == "weekly":
        if should_weekly_rebalance_now():
            targets = fetch_rebalancing_targets(rebalance_date)
            # 중복 실행 방지를 위해 즉시 스탬프(필요 시 FORCE로 재실행 가능)
            stamp_weekly_done()
            logger.info(
                f"[REBALANCE] 이번 주 리밸런싱 실행 기록 저장({_this_iso_week_key()})"
            )
        else:
            logger.info(
                "[REBALANCE] 이번 주 이미 실행됨 → 신규 리밸런싱 생략 (보유 관리만)"
            )
    else:
        # today/monthly 등 다른 앵커 모드는 기존 방식으로 바로 호출
        targets = fetch_rebalancing_targets(rebalance_date)

    # === [NEW] 예산 가드: 예수금이 0/부족이면 신규 매수만 스킵 ===
    effective_cash = _get_effective_ord_cash(kis, soft_cap=effective_capital)
    if effective_cash <= 0:
        can_buy = False
        logger.warning("[BUDGET] 유효 예산 0 → 신규 매수 스킵(보유 관리만 수행)")
    else:
        can_buy = True
    logger.info(
        f"[BUDGET] today effective cash = {effective_cash:,} KRW (capital base={effective_capital:,})"
    )

    # 리밸런싱 대상 후처리: qty 없고 weight만 있으면 배정 자본으로 수량 계산
    processed_targets: Dict[str, Any] = {}
    for t in targets:
        code = normalize_code(t.get("stock_code") or t.get("code"))
        if not code:
            continue
        name = t.get("name") or t.get("종목명")
        k_best = t.get("best_k") or t.get("K") or t.get("k")
        target_price = _to_float(t.get("목표가") or t.get("target_price"))
        qty = _to_int(t.get("매수수량") or t.get("qty"), 0)
        weight = t.get("weight")
        strategy = t.get("strategy") or "전월 rolling K 최적화"
        strategy_id = _normalize_strategy_id(_derive_strategy_id(t))
        logger.info(
            "[STRATEGY_ID_TARGET] code=%s strategy=%s strategy_id=%s",
            code,
            strategy,
            strategy_id,
        )
        avg_return_pct = _to_float(t.get("avg_return_pct") or t.get("수익률(%)"), 0.0)
        win_rate_pct = _to_float(t.get("win_rate_pct") or t.get("승률(%)"), 0.0)
        mdd_pct = _to_float(t.get("mdd_pct") or t.get("MDD(%)"), 0.0)
        trades = _to_int(t.get("trades"), 0)
        sharpe_m = _to_float(t.get("sharpe_m"), 0.0)
        cumret_pct = _to_float(
            t.get("cumulative_return_pct") or t.get("수익률(%)"), 0.0
        )

        if qty <= 0 and weight is not None:
            ref_px = _to_float(t.get("close")) or _to_float(t.get("prev_close"))
            try:
                qty = _weight_to_qty(
                    kis, code, float(weight), effective_capital, ref_price=ref_px
                )
            except Exception as e:
                logger.warning("[REBALANCE] weight→qty 변환 실패 %s: %s", code, e)
                qty = 0

        processed_targets[code] = {
            "code": code,
            "name": name,
            "best_k": k_best,
            "target_price": target_price,
            "qty": qty,
            "strategy": strategy,
            "strategy_id": strategy_id,
            "avg_return_pct": avg_return_pct,
            "win_rate_pct": win_rate_pct,
            "mdd_pct": mdd_pct,
            "trades": trades,
            "sharpe_m": sharpe_m,
            "cumulative_return_pct": cumret_pct,
            "prev_open": t.get("prev_open"),
            "prev_high": t.get("prev_high"),
            "prev_low": t.get("prev_low"),
            "prev_close": t.get("prev_close"),
            "prev_volume": t.get("prev_volume"),
        }

    # === 전략별 필터링 (전략 1~5 분리) ===
    # NOTE:
    # - 기존 코드가 모든 종목에 '챔피언 필터'를 강제 적용하면서 전략 1~3 진입이 사실상 막히는 문제가 있었음.
    # - 기본값은 전략 4(챔피언)에만 CHAMPION_* 필터를 적용하고, 나머지(1~3)는 메타 지표를 참고만 하도록 한다.
    strict_sid_env = _cfg("STRICT_CHAMPION_STRATEGY_IDS") or "4"
    strict_sids = {
        int(x.strip()) for x in str(strict_sid_env).split(",") if x.strip().isdigit()
    } or {4}

    filtered_targets: Dict[str, Any] = {}
    for code, info in processed_targets.items():
        sid = _normalize_strategy_id(info.get("strategy_id") or _derive_strategy_id(info))

        trades = _to_int(info.get("trades"), 0)
        win_rate = _to_float(info.get("win_rate_pct"), 0.0)
        mdd = abs(_to_float(info.get("mdd_pct"), 0.0) or 0.0)
        sharpe = _to_float(info.get("sharpe_m"), 0.0)

        # 전략4(챔피언) 등 '엄격 필터' 대상만 CHAMPION_* 기준으로 컷
        if sid in strict_sids:
            if (
                trades < CHAMPION_MIN_TRADES
                or win_rate < CHAMPION_MIN_WINRATE
                or mdd > CHAMPION_MAX_MDD
                or sharpe < CHAMPION_MIN_SHARPE
            ):
                logger.info(
                    "[STRATEGY_FILTER_SKIP] sid=%s code=%s trades=%s win=%.1f%% mdd=%.1f%% sharpe=%.2f",
                    sid,
                    code,
                    trades,
                    win_rate,
                    mdd,
                    sharpe,
                )
                continue

        filtered_targets[code] = info

        processed_targets = filtered_targets

        # 챔피언 등급화 (A/B/C) → 실제 매수 후보는 A급만 사용
        graded_targets: Dict[str, Any] = {}
        grade_counts = {"A": 0, "B": 0, "C": 0}
        for code, info in processed_targets.items():
            grade = _classify_champion_grade(info)
            info["champion_grade"] = grade
            graded_targets[code] = info
            grade_counts[grade] = grade_counts.get(grade, 0) + 1

        logger.info(
            "[CHAMPION-GRADE] A:%d / B:%d / C:%d (A/B급 실제 매수)",
            grade_counts.get("A", 0),
            grade_counts.get("B", 0),
            grade_counts.get("C", 0),
        )

        # 🔽 여기 필터를 A → A/B 로
        processed_targets = {
            k: v for k, v in graded_targets.items() if v.get("champion_grade") in ("A", "B")
        }
        # === [챔피언 & 레짐 상세 로그] ===
        try:
            if isinstance(processed_targets, dict) and len(processed_targets) > 0:
                # 1) 챔피언 1개(1순위)만 뽑아서 로그 (processed_targets가 dict일 때)
                first_code = next(iter(processed_targets.keys()))
                champion_one = processed_targets.get(first_code)

                # champion dict 안에 code가 없으면 보강
                if isinstance(champion_one, dict) and "code" not in champion_one:
                    champion_one = {**champion_one, "code": first_code}

                # 2) regime_state는 이미 만든 regime(또는 _update_market_regime(kis) 결과)를 넣어야 함
                #    이 블록 직전에 regime = _update_market_regime(kis) 가 있어야 함
                #    없으면 여기서 한 번 구해도 됨:
                try:
                    regime_state = regime  # regime 변수가 위에서 만들어져 있으면 이걸 사용
                except NameError:
                    regime_state = _update_market_regime(kis)

                # 3) context는 문자열로(혹은 execution.py를 Any로 바꿨다면 dict도 가능)
                context = "rebalance_api"

                log_champion_and_regime(logger, champion_one, regime_state, context)
        except Exception as e:
            logger.warning(f"[CHAMPION_LOG] 챔피언/레짐 로그 생성 실패: {e}")

        # 현재 레짐 기반 자본 스케일링 & 챔피언 선택
        selected_targets: Dict[str, Any] = {}
        regime = _update_market_regime(kis)
        pct_change = regime.get("pct_change") or 0.0
        mode = regime.get("mode") or "neutral"
        stage = regime.get("bear_stage") or 0
        regime_key = regime.get("key")
        R20 = regime.get("R20")
        D1 = regime.get("D1")

        REGIME_CAP_TABLE = {
            ("bull", 0): 1.0,
            ("neutral", 0): 0.8,
            ("bear", 0): 0.7,
            ("bear", 1): 0.5,
            ("bear", 2): 0.3,
        }

        REGIME_WEIGHTS = {
            ("bull", 0): [0.22, 0.20, 0.18, 0.16, 0.14, 0.10],
            ("neutral", 0): [0.20, 0.18, 0.16, 0.14, 0.12, 0.10, 0.10],
            ("bear", 0): [0.18, 0.16, 0.14, 0.12, 0.10],
            ("bear", 1): [0.16, 0.14, 0.12],
            ("bear", 2): [0.14, 0.12, 0.10],
        }

        REGIME_MAX_ACTIVE = {
            ("bull", 0): 6,
            ("neutral", 0): 5,
            ("bear", 0): 4,
            ("bear", 1): 3,
            ("bear", 2): 2,
        }

        REG_PARTIAL_S1 = float(_cfg("REG_PARTIAL_S1") or "0.3")
        REG_PARTIAL_S2 = float(_cfg("REG_PARTIAL_S2") or "0.3")
        TRAIL_PCT_BULL = float(_cfg("TRAIL_PCT_BULL") or "0.025")
        TRAIL_PCT_BEAR = float(_cfg("TRAIL_PCT_BEAR") or "0.012")
        TP_PROFIT_PCT_BULL = float(_cfg("TP_PROFIT_PCT_BULL") or "3.5")

        cap_scale = REGIME_CAP_TABLE.get(regime.get("key"), 0.8)
        ord_cash = _get_effective_ord_cash(kis, soft_cap=effective_capital)
        capital_base = min(ord_cash, int(CAP_CAP * effective_capital))
        capital_active = int(min(capital_base * cap_scale, effective_capital))
        logger.info(
            f"[REGIME-CAP] mode={mode} stage={stage} R20={R20 if R20 is not None else 'N/A'} "
            f"D1={D1 if D1 is not None else 'N/A'} "
            f"ord_cash(effective)={ord_cash:,} base={capital_base:,} active={capital_active:,} "
            f"scale={cap_scale:.2f}"
        )

        # 레짐별 최대 보유 종목 수
        n_active = REGIME_MAX_ACTIVE.get(
            regime_key, REGIME_MAX_ACTIVE.get(("neutral", 0), 3)
        )

        scored: List[Tuple[str, float, bool]] = []

        for code, info in processed_targets.items():
            score = _to_float(info.get("composite_score"), 0.0) or 0.0

            # 단기 모멘텀 강세 여부 (is_strong_momentum)로 버킷 구분
            try:
                strong = is_strong_momentum(kis, code)
            except Exception as e:
                logger.warning("[REBALANCE] 모멘텀 판별 실패 %s: %s", code, e)
                strong = False

            scored.append((code, score, strong))

        # 모멘텀 strong 버킷 우선, 그 다음 나머지 중에서 점수 순으로 채우기
        strong_bucket = [x for x in scored if x[2]]
        weak_bucket = [x for x in scored if not x[2]]

        strong_bucket.sort(key=lambda x: x[1], reverse=True)
        weak_bucket.sort(key=lambda x: x[1], reverse=True)

        picked: List[str] = []

        # 모멘텀 강 버킷을 우선 사용하되, 전체 보유 종목 수는 레짐별 n_active로 제한
        for code, score, _ in strong_bucket:
            if len(picked) >= n_active:
                break
            picked.append(code)

        for code, score, _ in weak_bucket:
            if len(picked) >= n_active:
                break
            picked.append(code)

        # === [NEW] 레짐별 챔피언 비중 & Target Notional 계산 ===
        regime_weights = REGIME_WEIGHTS.get(
            regime_key, REGIME_WEIGHTS.get(("neutral", 0), [1.0])
        )
        # 선택된 종목 수만큼 비중 슬라이스
        weights_for_picked: List[float] = list(regime_weights[: len(picked)])

        for idx, code in enumerate(picked):
            if code not in processed_targets:
                continue
            w = weights_for_picked[idx] if idx < len(weights_for_picked) else 0.0
            t = processed_targets[code]
            t["regime_weight"] = float(w)
            t["capital_active"] = int(capital_active)
            target_notional = int(round(capital_active * w))
            t["target_notional"] = target_notional

            ref_px = _to_float(t.get("close")) or _to_float(t.get("prev_close"))
            planned_qty = _notional_to_qty(kis, code, target_notional, ref_price=ref_px)
            t["qty"] = int(planned_qty)
            t["매수수량"] = int(planned_qty)
            processed_targets[code] = t

        for code in picked:
            if code in processed_targets:
                selected_targets[code] = processed_targets[code]

        logger.info(
            "[REGIME-CHAMPIONS] mode=%s stage=%s n_active=%s picked=%s capital_active=%s",
            mode,
            stage,
            n_active,
            picked,
            f"{capital_active:,}",
        )

        logger.info(
            "[REBALANCE] 레짐=%s pct=%.2f%%, 후보 %d개 중 상위 %d종목만 선택: %s",
            mode,
            pct_change,
            len(processed_targets),
            len(selected_targets),
            ",".join(selected_targets.keys()),
        )

        code_to_target: Dict[str, Any] = selected_targets

        # 눌림목 스캔용 코스닥 시총 상위 리스트 (챔피언과 별도로 관리)
        pullback_watch: Dict[str, Dict[str, Any]] = {}
        if USE_PULLBACK_ENTRY:
            try:
                pb_weight = max(0.0, min(PULLBACK_UNIT_WEIGHT, 1.0))
                base_notional = int(round(capital_active * pb_weight))
                pb_df = get_kosdaq_top_n(date_str=rebalance_date, n=PULLBACK_TOPN)
                for _, row in pb_df.iterrows():
                    code = normalize_code(row.get("Code") or row.get("code") or "")
                    if not code:
                        continue
                    pullback_watch[code] = {
                        "code": code,
                        "name": row.get("Name") or row.get("name"),
                        "notional": base_notional,
                    }
                logger.info(
                    f"[PULLBACK-WATCH] 코스닥 시총 Top{PULLBACK_TOPN} {len(pullback_watch)}종목 스캔 준비"
                )
            except Exception as e:
                logger.warning(f"[PULLBACK-WATCH-FAIL] 시총 상위 로드 실패: {e}")

        loop_sleep_sec = 2.5  # 메인 루프 대기 시간(초)
        max_closed_checks = 3
        closed_checks = 0

        try:
            while True:
                # === 코스닥 레짐 업데이트 ===
                regime = _update_market_regime(kis)
                regime_state = regime
                pct_txt = (
                    f"{regime.get('pct_change'):.2f}%"
                    if regime.get("pct_change") is not None
                    else "N/A"
                )
                logger.info(
                    f"[REGIME] mode={regime['mode']} stage={regime['bear_stage']} pct={pct_txt}"
                )

                # 장 상태
                now_dt_kst = datetime.now(KST)
                is_open = kis.is_market_open()
                now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")
                today_prefix = now_dt_kst.strftime("%Y-%m-%d")
                _ensure_guard_state(now_dt_kst.date())
                if last_today_prefix != today_prefix:
                    triggered_today.clear()
                    s1_done_today.clear()
                    last_today_prefix = today_prefix
                expired_pending = _cleanup_expired_pending(traded, now_dt_kst, ttl_sec=300)
                if expired_pending:
                    triggered_today.difference_update(expired_pending)
                traded_today: set[str] = set()
                regime_s1_summary = {
                    "sent_qty": 0,
                    "sent_orders": 0,
                    "skipped": 0,
                    "total_qty": 0,
                    "by_stock": {},
                }

                def _log_s1_action(
                    code: str,
                    strategy_id: int | str,
                    status: str,
                    base_qty: int,
                    target_qty: int,
                    sold_today: int,
                    remaining: int,
                    sell_qty: int,
                    reason_msg: str | None = None,
                ) -> None:
                    key = f"{normalize_code(code)}:{strategy_id}"
                    regime_s1_summary["by_stock"][key] = {
                        "status": status,
                        "base_qty": int(base_qty),
                        "target": int(target_qty),
                        "sold_today": int(sold_today),
                        "remaining": int(remaining),
                        "sell_qty": int(sell_qty),
                        "reason": reason_msg or None,
                    }
                    prefix = (
                        "[SELL][SENT]"
                        if status == "SENT"
                        else "[SELL][SKIP]" if status == "SKIP" else "[SELL][ERROR]"
                    )
                    msg = (
                        f"{prefix} [REGIME_S1] {code}:{strategy_id} base_qty={base_qty} target={target_qty} "
                        f"sold={sold_today} remaining={remaining} sell_qty={sell_qty}"
                    )
                    if reason_msg:
                        msg += f" reason={reason_msg}"
                    if status == "ERROR":
                        logger.error(msg)
                    else:
                        logger.info(msg)

                def _log_s2_action(
                    code: str,
                    strategy_id: int | str,
                    status: str,
                    base_qty: int,
                    target_qty: int,
                    sold_today: int,
                    remaining: int,
                    sell_qty: int,
                    reason_msg: str | None = None,
                ) -> None:
                    prefix = (
                        "[SELL][SENT]"
                        if status == "SENT"
                        else "[SELL][SKIP]" if status == "SKIP" else "[SELL][ERROR]"
                    )
                    msg = (
                        f"{prefix} [REGIME_S2] {code}:{strategy_id} base_qty={base_qty} target={target_qty} "
                        f"sold={sold_today} remaining={remaining} sell_qty={sell_qty}"
                    )
                    if reason_msg:
                        msg += f" reason={reason_msg}"
                    if status == "ERROR":
                        logger.error(msg)
                    else:
                        logger.info(msg)

                def _strategy_ids_for_code(code: str) -> list[str]:
                    code_key = normalize_code(code)
                    totals: dict[str, int] = {}
                    lots = lot_state.get("lots", [])
                    if isinstance(lots, list):
                        for lot in lots:
                            if normalize_code(lot.get("pdno")) != code_key:
                                continue
                            remaining = int(lot.get("remaining_qty") or 0)
                            if remaining <= 0:
                                continue
                            sid = lot.get("strategy_id")
                            if sid is None:
                                continue
                            if str(sid).isdigit():
                                sid_int = int(sid)
                                if 1 <= sid_int <= 5:
                                    totals[str(sid_int)] = totals.get(str(sid_int), 0) + remaining
                    ordered: list[str] = []
                    for sid in STRATEGY_REDUCTION_PRIORITY:
                        key = str(sid)
                        if key in totals:
                            ordered.append(key)
                    for sid in sorted(totals.keys()):
                        if sid not in ordered:
                            ordered.append(sid)
                    return ordered

                def _run_bear_reduction(
                    code: str,
                    *,
                    is_target: bool,
                    regime: dict[str, Any],
                ) -> None:
                    sellable_qty = ord_psbl_map.get(code, 0)
                    if sellable_qty <= 0:
                        return
                    for sid in _strategy_ids_for_code(code):
                        remaining_strategy = remaining_qty_for_strategy(lot_state, code, sid)
                        if remaining_strategy <= 0:
                            continue
                        entry = _ensure_position_entry(code, sid)
                        flags = entry.setdefault(
                            "flags",
                            {"bear_s1_done": False, "bear_s2_done": False, "sold_p1": False, "sold_p2": False},
                        )
                        if regime.get("bear_stage", 0) >= 1:
                            if flags.get("bear_s1_done"):
                                continue
                            guard = (
                                _s1_guard_target(now_dt_kst.date(), code, sid, remaining_strategy)
                                if is_target
                                else _s1_guard_nontarget(now_dt_kst.date(), code, sid, remaining_strategy)
                            )
                            base_qty = int(guard.get("base_qty") or 0)
                            if base_qty <= 0:
                                regime_s1_summary["skipped"] += 1
                                _log_s1_action(
                                    code,
                                    sid,
                                    "SKIP",
                                    base_qty,
                                    0,
                                    int(guard.get("sold", 0)),
                                    0,
                                    0,
                                    "base_qty_zero",
                                )
                            else:
                                target_qty = max(1, int(base_qty * REG_PARTIAL_S1))
                                sold_today = int(guard.get("sold", 0))
                                remaining = max(0, target_qty - sold_today)

                                if remaining <= 0 or sellable_qty <= 0:
                                    if remaining <= 0:
                                        _set_position_flags(code, sid, bear_s1_done=True)
                                        s1_done_today.add((normalize_code(code), str(sid)))
                                    regime_s1_summary["skipped"] += 1
                                    _log_s1_action(
                                        code,
                                        sid,
                                        "SKIP",
                                        base_qty,
                                        target_qty,
                                        sold_today,
                                        remaining,
                                        0,
                                        "target_met" if remaining <= 0 else "no_sellable_qty",
                                    )
                                else:
                                    sell_qty = min(remaining, sellable_qty, remaining_strategy)
                                    sell_qty = _cap_sell_qty(code, sell_qty)
                                    if sell_qty <= 0:
                                        regime_s1_summary["skipped"] += 1
                                        _log_s1_action(
                                            code,
                                            sid,
                                            "SKIP",
                                            base_qty,
                                            target_qty,
                                            sold_today,
                                            remaining,
                                            0,
                                            "strategy_qty_zero",
                                        )
                                        continue
                                    regime_s1_summary["total_qty"] += int(sell_qty)
                                    try:
                                        prev_qty_before = int(
                                            (holding.get(code) or {}).get("qty") or 0
                                        )
                                        if dry_run:
                                            logger.info(
                                                "[DRY-RUN][SELL] code=%s qty=%s strategy_id=%s reason=%s",
                                                code,
                                                sell_qty,
                                                sid,
                                                reason_msg,
                                            )
                                            runtime_state_store.mark_order(
                                                runtime_state,
                                                code,
                                                "SELL",
                                                sid,
                                                int(sell_qty),
                                                float(holding.get(code, {}).get("buy_price") or 0.0),
                                                now_dt_kst.isoformat(),
                                                status="submitted(dry)",
                                            )
                                            _save_runtime_state()
                                            status, skip_reason = "SKIP", "DRY_RUN"
                                            exec_px, result = None, {"status": "SKIPPED", "skip_reason": "DRY_RUN"}
                                        else:
                                            exec_px, result = _sell_once(
                                                kis, code, sell_qty, prefer_market=True
                                            )
                                            runtime_state_store.mark_order(
                                                runtime_state,
                                                code,
                                                "SELL",
                                                sid,
                                                int(sell_qty),
                                                float(exec_px or 0.0),
                                                now_dt_kst.isoformat(),
                                                status="submitted",
                                            )
                                            _save_runtime_state()
                                        status, skip_reason = _sell_result_status(result)
                                    except Exception as e:
                                        exec_px, result = None, None
                                        status, skip_reason = "ERROR", str(e)

                                    reason_msg = skip_reason or (
                                        "시장약세 1단계 축소"
                                        if is_target
                                        else "시장약세 1단계 축소(비타겟)"
                                    )

                                    if status == "SENT":
                                        guard["sold"] = sold_today + int(sell_qty)
                                        holding[code]["qty"] = max(
                                            0, holding[code]["qty"] - int(sell_qty)
                                        )
                                        if guard["sold"] >= target_qty:
                                            _set_position_flags(code, sid, bear_s1_done=True)
                                            s1_done_today.add((normalize_code(code), str(sid)))
                                        _persist_guard_state(now_dt_kst.date())
                                        regime_s1_summary["sent_qty"] += int(sell_qty)
                                        regime_s1_summary["sent_orders"] += 1
                                        trade_payload = {
                                            "datetime": now_str,
                                            "code": code,
                                            "name": None,
                                            "qty": int(sell_qty),
                                            "K": holding[code].get("k_value"),
                                            "target_price": holding[code].get("target_price_src"),
                                            "strategy": "레짐축소" if is_target else "기존보유 능동관리",
                                            "side": "SELL",
                                            "price": exec_px,
                                            "amount": int((exec_px or 0)) * int(sell_qty),
                                            "reason": reason_msg,
                                        }
                                        if result is not None:
                                            trade_payload["result"] = result
                                        log_trade(trade_payload)
                                        _apply_sell_to_ledger_with_balance(
                                            code,
                                            int(sell_qty),
                                            now_dt_kst.isoformat(),
                                            result,
                                            scope="strategy",
                                            trigger_strategy_id=int(sid) if sid.isdigit() else sid,
                                            prev_qty_before=prev_qty_before,
                                        )
                                        runtime_state_store.mark_fill(
                                            runtime_state,
                                            code,
                                            "SELL",
                                            sid,
                                            int(sell_qty),
                                            float(exec_px or 0.0),
                                            now_dt_kst.isoformat(),
                                            status="filled",
                                        )
                                        _save_runtime_state()
                                        save_state(holding, traded)
                                        time.sleep(RATE_SLEEP_SEC)
                                        sellable_qty = max(0, int(sellable_qty) - int(sell_qty))
                                    elif status == "SKIP":
                                        regime_s1_summary["skipped"] += 1
                                    else:
                                        regime_s1_summary["skipped"] += 1

                                    _log_s1_action(
                                        code,
                                        sid,
                                        status,
                                        base_qty,
                                        target_qty,
                                        sold_today,
                                        remaining,
                                        sell_qty,
                                        reason_msg,
                                    )

                        if regime.get("bear_stage", 0) >= 2:
                            if flags.get("bear_s2_done"):
                                continue
                            if not flags.get("bear_s1_done"):
                                _log_s2_action(
                                    code,
                                    sid,
                                    "SKIP",
                                    int(remaining_strategy),
                                    0,
                                    0,
                                    0,
                                    0,
                                    "s1_not_done",
                                )
                                continue
                            if (normalize_code(code), str(sid)) in s1_done_today:
                                logger.warning(
                                    "[REGIME_S2][SEQ] %s:%s 동일 일자 S1 완료 직후 S2 진입",
                                    normalize_code(code),
                                    sid,
                                )
                            sellable_qty = ord_psbl_map.get(code, 0)
                            remaining_strategy_stage2 = remaining_qty_for_strategy(
                                lot_state, code, sid
                            )
                            guard = (
                                _s2_guard_target(
                                    now_dt_kst.date(), code, sid, remaining_strategy_stage2
                                )
                                if is_target
                                else _s2_guard_nontarget(
                                    now_dt_kst.date(), code, sid, remaining_strategy_stage2
                                )
                            )
                            base_qty = int(guard.get("base_qty") or 0)
                            if base_qty <= 0:
                                _log_s2_action(
                                    code,
                                    sid,
                                    "SKIP",
                                    base_qty,
                                    0,
                                    int(guard.get("sold", 0)),
                                    0,
                                    0,
                                    "base_qty_zero",
                                )
                            else:
                                target_qty = max(1, int(base_qty * REG_PARTIAL_S2))
                                sold_today = int(guard.get("sold", 0))
                                remaining = max(0, target_qty - sold_today)

                                if remaining <= 0 or sellable_qty <= 0:
                                    if remaining <= 0:
                                        _set_position_flags(code, sid, bear_s2_done=True)
                                    _log_s2_action(
                                        code,
                                        sid,
                                        "SKIP",
                                        base_qty,
                                        target_qty,
                                        sold_today,
                                        remaining,
                                        0,
                                        "target_met" if remaining <= 0 else "no_sellable_qty",
                                    )
                                else:
                                    sell_qty = min(
                                        remaining, sellable_qty, remaining_strategy_stage2
                                    )
                                    sell_qty = _cap_sell_qty(code, sell_qty)
                                    if sell_qty <= 0:
                                        _log_s2_action(
                                            code,
                                            sid,
                                            "SKIP",
                                            base_qty,
                                            target_qty,
                                            sold_today,
                                            remaining,
                                            0,
                                            "strategy_qty_zero",
                                        )
                                        continue
                                    try:
                                        prev_qty_before = int(
                                            (holding.get(code) or {}).get("qty") or 0
                                        )
                                        if dry_run:
                                            logger.info(
                                                "[DRY-RUN][SELL] code=%s qty=%s strategy_id=%s reason=%s",
                                                code,
                                                sell_qty,
                                                sid,
                                                reason_msg,
                                            )
                                            runtime_state_store.mark_order(
                                                runtime_state,
                                                code,
                                                "SELL",
                                                sid,
                                                int(sell_qty),
                                                float(holding.get(code, {}).get("buy_price") or 0.0),
                                                now_dt_kst.isoformat(),
                                                status="submitted(dry)",
                                            )
                                            _save_runtime_state()
                                            status, skip_reason = "SKIP", "DRY_RUN"
                                            exec_px, result = None, {"status": "SKIPPED", "skip_reason": "DRY_RUN"}
                                        else:
                                            exec_px, result = _sell_once(
                                                kis, code, sell_qty, prefer_market=True
                                            )
                                            runtime_state_store.mark_order(
                                                runtime_state,
                                                code,
                                                "SELL",
                                                sid,
                                                int(sell_qty),
                                                float(exec_px or 0.0),
                                                now_dt_kst.isoformat(),
                                                status="submitted",
                                            )
                                            _save_runtime_state()
                                        status, skip_reason = _sell_result_status(result)
                                    except Exception as e:
                                        exec_px, result = None, None
                                        status, skip_reason = "ERROR", str(e)

                                    reason_msg = skip_reason or (
                                        "시장약세 2단계 축소"
                                        if is_target
                                        else "시장약세 2단계 축소(비타겟)"
                                    )

                                    if status == "SENT":
                                        guard["sold"] = sold_today + int(sell_qty)
                                        holding[code]["qty"] = max(
                                            0, holding[code]["qty"] - int(sell_qty)
                                        )
                                        if guard["sold"] >= target_qty:
                                            _set_position_flags(code, sid, bear_s2_done=True)
                                        _persist_guard_state(now_dt_kst.date())
                                        log_trade(
                                            {
                                                "datetime": now_str,
                                                "code": code,
                                                "name": None,
                                                "qty": int(sell_qty),
                                                "K": holding[code].get("k_value"),
                                                "target_price": holding[code].get("target_price_src"),
                                                "strategy": "레짐축소" if is_target else "기존보유 능동관리",
                                                "side": "SELL",
                                                "price": exec_px,
                                                "amount": int((exec_px or 0)) * int(sell_qty),
                                                "result": result,
                                                "reason": reason_msg,
                                            }
                                        )
                                        _apply_sell_to_ledger_with_balance(
                                            code,
                                            int(sell_qty),
                                            now_dt_kst.isoformat(),
                                            result,
                                            scope="strategy",
                                            trigger_strategy_id=int(sid) if sid.isdigit() else sid,
                                            prev_qty_before=prev_qty_before,
                                        )
                                        runtime_state_store.mark_fill(
                                            runtime_state,
                                            code,
                                            "SELL",
                                            sid,
                                            int(sell_qty),
                                            float(exec_px or 0.0),
                                            now_dt_kst.isoformat(),
                                            status="filled",
                                        )
                                        _save_runtime_state()
                                        save_state(holding, traded)
                                        time.sleep(RATE_SLEEP_SEC)
                                        sellable_qty = max(0, int(sellable_qty) - int(sell_qty))
                                    elif status == "SKIP":
                                        pass

                                    _log_s2_action(
                                        code,
                                        sid,
                                        status,
                                        base_qty,
                                        target_qty,
                                        sold_today,
                                        remaining,
                                        sell_qty,
                                        reason_msg,
                                    )

                if now_dt_kst.date() != pullback_buy_date:
                    pullback_buy_date = now_dt_kst.date()
                    pullback_buys_today = 0

                if not is_open:
                    if not is_trading_day(now_dt_kst):
                        logger.error("[CLOSED] 비거래일 감지 → 루프 종료")
                        break

                    if now_dt_kst.time() < MARKET_OPEN:
                        seconds_to_open = int(
                            (
                                datetime.combine(now_dt_kst.date(), MARKET_OPEN, tzinfo=KST)
                                - now_dt_kst
                            ).total_seconds()
                        )
                        sleep_for = max(1, min(seconds_to_open, 300))
                        logger.info(
                            "[PREOPEN] 장 시작까지 %ss 남음 → %ss 대기 후 재확인",
                            seconds_to_open,
                            sleep_for,
                        )
                        time.sleep(sleep_for)
                        closed_checks = 0
                        continue

                    if now_dt_kst.time() >= MARKET_CLOSE:
                        logger.error("[CLOSED] 장 마감 이후 → 루프 종료")
                        break

                    closed_checks += 1
                    if not ALLOW_WHEN_CLOSED:
                        if closed_checks > max_closed_checks:
                            logger.error(
                                "[CLOSED] 장 종료 반복 %s회 초과 → 루프 종료",
                                max_closed_checks,
                            )
                            break
                        logger.info(
                            "[CLOSED] 장중인데 API가 닫힘 응답 → 10초 대기 후 재확인 (%s/%s)",
                            closed_checks,
                            max_closed_checks,
                        )
                        time.sleep(10)
                        continue
                    else:
                        logger.warning(
                            "[CLOSED-DATA] 장 종료지만 환경설정 허용 → 시세 조회 후 진행"
                        )
                else:
                    closed_checks = 0

                if kis.should_cooldown(now_dt_kst):
                    logger.warning("[COOLDOWN] 2초간 대기 (API 제한 보호)")
                    time.sleep(2)

                # 잔고 가져오기
                prev_holding = holding if isinstance(holding, dict) else {}
                balances = _fetch_balances(kis)
                holding = {}
                for bal in balances:
                    code = normalize_code(bal.get("code") or bal.get("pdno"))
                    qty = int(bal.get("qty", 0))
                    if qty <= 0:
                        continue
                    price = float(bal.get("avg_price", 0.0))
                    holding[code] = {
                        "qty": qty,
                        "buy_price": price,
                        "bear_s1_done": False,
                        "bear_s2_done": False,
                    }
                    _init_position_state_from_balance(kis, holding, code, price, qty)

                before_lot_signature = _lot_state_signature(lot_state)
                reconcile_with_broker_holdings(lot_state, balances)
                _maybe_save_lot_state(before_lot_signature)

                position_state = reconcile_with_broker(
                    position_state, balances, lot_state=lot_state
                )
                position_state = reconcile_positions(
                    balances, position_state, _load_trade_log(), processed_targets.keys()
                )
                position_state_dirty = True

                for code, info in holding.items():
                    pos_state = position_state.get("positions", {}).get(normalize_code(code))
                    if not isinstance(pos_state, dict):
                        continue
                    strategies = pos_state.get("strategies", {})
                    if not strategies:
                        _ensure_position_entry(code, "MANUAL")
                        position_state_dirty = True
                        strategies = pos_state.get("strategies", {})
                    entry = next(iter(strategies.values()), None)
                    if not isinstance(entry, dict):
                        continue
                    meta = entry.get("meta", {})
                    info["engine"] = entry.get("entry", {}).get("engine") or info.get("engine")
                    info["pullback_peak_price"] = meta.get("pullback_peak_price")
                    info["pullback_reversal_price"] = meta.get("pullback_reversal_price")

                # 잔고 기준으로 보유종목 매도 가능 수량 맵 생성
                ord_psbl_map = {
                    normalize_code(bal.get("code") or bal.get("pdno")): int(
                        bal.get("sell_psbl_qty", 0)
                    )
                    for bal in balances
                }

                if isinstance(traded, dict):
                    for code, payload in list(traded.items()):
                        if (payload or {}).get("status") == "pending" and code in holding:
                            traded[code]["status"] = "filled"

                traded_today = _traded_today(traded, today_prefix)
                for bal in balances:
                    code = normalize_code(bal.get("code") or bal.get("pdno"))
                    raw = bal.get("raw") or {}
                    raw_l = {str(k).lower(): v for k, v in raw.items()}
                    thdt_buy_qty = _to_int(
                        raw_l.get("thdt_buyqty")
                        or raw_l.get("thdt_buy_qty")
                        or raw_l.get("thdt_buy_q")
                    )
                    if thdt_buy_qty > 0:
                        traded_today.add(code)

                if not ALLOW_PYRAMID:
                    traded_today.update(holding.keys())

                for code, info in list(holding.items()):
                    prev_qty = int(
                        (prev_holding.get(code) or {}).get("qty", info.get("qty", 0))
                    )
                    balance_qty = int(info.get("qty", 0))
                    # 잔고가 일시적으로 줄어든 케이스만 보호하고, 정상적인 수량 증가는 유지한다.
                    if prev_qty > 0 and 0 < balance_qty < prev_qty:
                        holding[code]["qty"] = prev_qty
                        logger.info(
                            f"[HOLDING-QTY-CLAMP] {code}: balance_qty={balance_qty} prev_qty={prev_qty} → {prev_qty}"
                        )

                recent_keep_minutes = 5
                for code, info in prev_holding.items():
                    if code in holding:
                        continue
                    buy_time_str = None
                    if isinstance(traded, dict):
                        buy_time_str = (traded.get(code) or {}).get("buy_time")
                    if buy_time_str:
                        try:
                            buy_dt = datetime.strptime(buy_time_str, "%Y-%m-%d %H:%M:%S")
                            buy_dt = buy_dt.replace(tzinfo=now_dt_kst.tzinfo)
                            if now_dt_kst - buy_dt <= timedelta(
                                minutes=recent_keep_minutes
                            ):
                                holding[code] = info
                                ord_psbl_map.setdefault(code, int(info.get("qty", 0)))
                                logger.info(
                                    f"[HOLDING-MERGE] {code} 최근 매수({buy_time_str}) 반영 → 잔고 미반영 보호"
                                )
                        except Exception as e:
                            logger.warning(f"[HOLDING-MERGE-FAIL] {code}: {e}")

                logger.info(
                    f"[STATUS] holdings={holding} traded_today={sorted(traded_today)} ord_psbl={ord_psbl_map}"
                )

                # 커트오프 타임 도달 시 강제매도 루틴
                force_global_liquidation = (
                    EMERGENCY_GLOBAL_SELL
                    or (SELL_ALL_BALANCES_AT_CUTOFF and now_dt_kst.time() >= SELL_FORCE_TIME)
                )
                if force_global_liquidation:
                    logger.info("[⏰ 강제 전량매도 루틴 실행] emergency=%s cutoff=%s", EMERGENCY_GLOBAL_SELL, SELL_ALL_BALANCES_AT_CUTOFF)
                    pass_count = FORCE_SELL_PASSES_CUTOFF
                    if now_dt_kst.time() >= dtime(hour=15, minute=0):
                        pass_count = FORCE_SELL_PASSES_CLOSE
                    for code, qty in ord_psbl_map.items():
                        if qty <= 0:
                            continue
                        prev_qty_before = int((holding.get(code) or {}).get("qty") or 0)
                        qty = _cap_sell_qty(code, qty)
                        if qty <= 0:
                            continue
                        if dry_run:
                            logger.info(
                                "[DRY-RUN][SELL] code=%s qty=%s strategy_id=GLOBAL reason=force_liquidation",
                                code,
                                qty,
                            )
                            runtime_state_store.mark_order(
                                runtime_state,
                                code,
                                "SELL",
                                "GLOBAL",
                                int(qty),
                                float(holding.get(code, {}).get("buy_price") or 0.0),
                                now_dt_kst.isoformat(),
                                status="submitted(dry)",
                            )
                            _save_runtime_state()
                            continue
                        exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
                        runtime_state_store.mark_order(
                            runtime_state,
                            code,
                            "SELL",
                            "GLOBAL",
                            int(qty),
                            float(exec_px or 0.0),
                            now_dt_kst.isoformat(),
                            status="submitted",
                        )
                        _save_runtime_state()
                        log_trade(
                            {
                                "datetime": now_str,
                                "code": code,
                                "name": None,
                                "qty": int(qty),
                                "K": None,
                                "target_price": None,
                                "strategy": "강제매도",
                                "side": "SELL",
                                "price": exec_px,
                                "amount": int((exec_px or 0)) * int(qty),
                                "result": result,
                                "reason": "커트오프 강제매도",
                            }
                        )
                        _apply_sell_to_ledger_with_balance(
                            code,
                            int(qty),
                            now_dt_kst.isoformat(),
                            result,
                            scope="global",
                            trigger_strategy_id=None,
                            prev_qty_before=prev_qty_before,
                            allow_blocked=FORCE_SELL_BLOCKED_LOTS,
                        )
                        runtime_state_store.mark_fill(
                            runtime_state,
                            code,
                            "SELL",
                            "GLOBAL",
                            int(qty),
                            float(exec_px or 0.0),
                            now_dt_kst.isoformat(),
                            status="filled",
                        )
                        _save_runtime_state()
                        time.sleep(RATE_SLEEP_SEC)
                    for _ in range(pass_count - 1):
                        logger.info(
                            f"[커트오프 추가패스] {pass_count}회 중 남은 패스 실행 (잔고변동 감지용)"
                        )
                        time.sleep(loop_sleep_sec)
                        continue
                    logger.info("[⏰ 커트오프 종료] 루프 종료")
                    break

                # === (1) 잔여 물량 대상 스탑/리밸런스 관리 ===
                for code in list(holding.keys()):
                    code_key = normalize_code(code)
                    pos_state = position_state.get("positions", {}).get(code_key)
                    entries = pos_state.get("strategies", {}) if isinstance(pos_state, dict) else {}
                    logger.info(
                        "[EXIT-CHECK] code=%s positions=%s",
                        code_key,
                        len(entries),
                    )
                    cur_price = _safe_get_price(kis, code_key)
                    for sid, entry in entries.items():
                        if not isinstance(entry, dict):
                            continue
                        avg_price = strategy_avg_price(lot_state, code_key, sid)
                        entry_meta = entry.get("meta", {}) or {}
                        high = float(entry.get("high_watermark") or entry_meta.get("high") or 0.0)
                        if avg_price is not None:
                            high = max(high, float(avg_price))
                        flags = entry.get("flags", {}) or {}
                        avg_label = f"{avg_price:.2f}" if avg_price is not None else None
                        high_label = f"{high:.2f}" if high else None
                        pnl_pct = None
                        if cur_price and avg_price:
                            pnl_pct = (float(cur_price) - float(avg_price)) / float(avg_price) * 100.0
                        pnl_label = f"{pnl_pct:.2f}" if pnl_pct is not None else None
                        logger.info(
                            "  - sid=%s qty=%s avg=%s high=%s pnl%%=%s flags=%s engine=%s",
                            sid,
                            entry.get("qty"),
                            avg_label,
                            high_label,
                            pnl_label,
                            flags,
                            entry.get("engine") or entry.get("entry", {}).get("engine"),
                        )
                    # 신규 진입 금지 모드
                    if code not in code_to_target:
                        continue

                    # --- 1a) 강제 레짐별 축소 로직 ---
                    sellable_qty = ord_psbl_map.get(code, 0)
                    if sellable_qty <= 0:
                        continue

                    regime_key = regime.get("key")
                    mode = regime.get("mode")
                    if regime_key and regime_key[0] == "bear":
                        _run_bear_reduction(code, is_target=True, regime=regime)

                    # --- 1b) TP/SL/트레일링, VWAP 가드 ---
                    try:
                        exit_intents = _build_exit_intents(
                            code, mode or "neutral"
                        )
                    except Exception as e:
                        logger.error(f"[_adaptive_exit 실패] {code}: {e}")
                        exit_intents = []

                    for intent in exit_intents:
                        sell_qty = int(intent.get("sell_qty") or 0)
                        if sell_qty <= 0:
                            continue
                        sid = intent.get("strategy_id")
                        sell_qty = _cap_sell_qty(code, sell_qty)
                        if sell_qty <= 0:
                            continue
                        if dry_run:
                            logger.info(
                                "[DRY-RUN][SELL] code=%s qty=%s strategy_id=%s reason=%s",
                                code,
                                sell_qty,
                                sid,
                                intent.get("reason"),
                            )
                            runtime_state_store.mark_order(
                                runtime_state,
                                code,
                                "SELL",
                                sid,
                                int(sell_qty),
                                float(holding.get(code, {}).get("buy_price") or 0.0),
                                now_dt_kst.isoformat(),
                                status="submitted(dry)",
                            )
                            _save_runtime_state()
                            continue
                        prev_qty_before = int((holding.get(code) or {}).get("qty") or 0)
                        exec_px, result = _sell_once(
                            kis, code, sell_qty, prefer_market=True
                        )
                        runtime_state_store.mark_order(
                            runtime_state,
                            code,
                            "SELL",
                            sid,
                            int(sell_qty),
                            float(exec_px or 0.0),
                            now_dt_kst.isoformat(),
                            status="submitted",
                        )
                        _save_runtime_state()
                        log_trade(
                            {
                                "datetime": now_str,
                                "code": code,
                                "name": None,
                                "qty": int(sell_qty),
                                "K": None,
                                "target_price": None,
                                "strategy": f"adaptive_exit_{sid}",
                                "side": "SELL",
                                "price": exec_px,
                                "amount": int((exec_px or 0)) * int(sell_qty),
                                "result": result,
                                "reason": intent.get("reason"),
                            }
                        )
                        _apply_sell_to_ledger_with_balance(
                            code,
                            int(sell_qty),
                            now_dt_kst.isoformat(),
                            result,
                            scope="strategy",
                            trigger_strategy_id=int(sid) if sid is not None and str(sid).isdigit() else sid,
                            prev_qty_before=prev_qty_before,
                        )
                        runtime_state_store.mark_fill(
                            runtime_state,
                            code,
                            "SELL",
                            sid,
                            int(sell_qty),
                            float(exec_px or 0.0),
                            now_dt_kst.isoformat(),
                            status="filled",
                        )
                        _save_runtime_state()
                        save_state(holding, traded)
                        time.sleep(RATE_SLEEP_SEC)

                    if not exit_intents:
                        try:
                            current_price = _safe_get_price(kis, code)
                        except Exception:
                            current_price = None
                        if current_price and _pullback_stop_hit(code, current_price):
                            sellable_qty = ord_psbl_map.get(code, 0)
                            pb_avail = remaining_qty_for_strategy(lot_state, code, 5)
                            sell_qty = min(int(sellable_qty), int(pb_avail))
                            if sell_qty > 0:
                                prev_qty_before = int(
                                    (holding.get(code) or {}).get("qty") or 0
                                )
                                if dry_run:
                                    logger.info(
                                        "[DRY-RUN][SELL] code=%s qty=%s strategy_id=5 reason=pullback_reversal_break",
                                        code,
                                        sell_qty,
                                    )
                                    runtime_state_store.mark_order(
                                        runtime_state,
                                        code,
                                        "SELL",
                                        5,
                                        int(sell_qty),
                                        float(holding.get(code, {}).get("buy_price") or 0.0),
                                        now_dt_kst.isoformat(),
                                        status="submitted(dry)",
                                    )
                                    _save_runtime_state()
                                    continue
                                exec_px, result = _sell_once(
                                    kis, code, sell_qty, prefer_market=True
                                )
                                runtime_state_store.mark_order(
                                    runtime_state,
                                    code,
                                    "SELL",
                                    5,
                                    int(sell_qty),
                                    float(exec_px or 0.0),
                                    now_dt_kst.isoformat(),
                                    status="submitted",
                                )
                                _save_runtime_state()
                                log_trade(
                                    {
                                        "datetime": now_str,
                                        "code": code,
                                        "name": None,
                                        "qty": int(sell_qty),
                                        "K": None,
                                        "target_price": None,
                                        "strategy": "눌림목 손절",
                                        "side": "SELL",
                                        "price": exec_px,
                                        "amount": int((exec_px or 0)) * int(sell_qty),
                                        "result": result,
                                        "reason": "pullback_reversal_break",
                                    }
                                )
                                _apply_sell_to_ledger_with_balance(
                                    code,
                                    int(sell_qty),
                                    now_dt_kst.isoformat(),
                                    result,
                                    scope="strategy",
                                    trigger_strategy_id=5,
                                    prev_qty_before=prev_qty_before,
                                )
                                runtime_state_store.mark_fill(
                                    runtime_state,
                                    code,
                                    "SELL",
                                    5,
                                    int(sell_qty),
                                    float(exec_px or 0.0),
                                    now_dt_kst.isoformat(),
                                    status="filled",
                                )
                                _save_runtime_state()
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)
                                logger.info(
                                    "[PULLBACK-STOP] code=%s current=%s reason=reversal_break",
                                    code,
                                    current_price,
                                )

                # === (2) 신규 진입 로직 (챔피언) ===
                for code, info in code_to_target.items():
                    if not can_buy:
                        continue

                    if code in traded_today:
                        continue

                    if code in holding and not ALLOW_PYRAMID:
                        continue

                    if code in triggered_today:
                        logger.info(f"[TRIGGER-SKIP] {code}: 금일 이미 트리거 발생")
                        continue

                    target_qty = int(info.get("qty", 0))
                    if target_qty <= 0:
                        logger.info(f"[REBALANCE] {code}: target_qty=0 → 스킵")
                        continue

                    target_price = info.get("target_price")
                    k_value = info.get("best_k")
                    strategy = info.get("strategy")
                    weight = _to_float(info.get("weight") or 0.0)

                    planned_notional = int(
                        _to_float(info.get("target_notional") or 0.0) or 0
                    )
                    logger.info(
                        f"[TARGET] {code} qty={target_qty} tgt_px={target_price} notional={planned_notional} K={k_value}"
                    )

                    # [중복 진입 방지] 이미 주문된 종목인지 확인
                    if code in traded_today:
                        logger.info(f"[SKIP] {code}: 이미 금일 거래됨")
                        continue

                    # strategy_id는 위에서 strategy_entry_gate에서 이미 정규화/결정됨
                    if strategy_id is not None and remaining_qty_for_strategy(
                        lot_state, code, strategy_id
                    ) > 0:
                        logger.info(
                            "[ENTRY-SKIP] already owned in ledger: code=%s sid=%s",
                            code,
                            strategy_id,
                        )
                        continue

                    if _pending_block(traded, code, now_dt_kst, block_sec=45):
                        logger.info(
                            f"[SKIP-PENDING] {code}: pending 쿨다운 중 → 재주문 방지"
                        )
                        continue
                    if runtime_state_store.should_block_order(
                        runtime_state, code, "BUY", now_dt_kst.isoformat()
                    ):
                        logger.info(
                            "[IDEMPOTENT-SKIP] %s BUY blocked within window",
                            code,
                        )
                        continue

                    prev_price = (
                        position_state.get("memory", {})
                        .get("last_price", {})
                        .get(normalize_code(code))
                    )
                    if prev_price is None:
                        try:
                            cached = signals._LAST_PRICE_CACHE.get(code) or {}
                            ts = cached.get("ts")
                            if ts and (time.time() - float(ts) <= 120):
                                prev_price = cached.get("px")
                        except Exception:
                            prev_price = None

                    price_res = _safe_get_price(kis, code, with_source=True)
                    if isinstance(price_res, tuple):
                        current_price, price_source = price_res
                    else:
                        current_price, price_source = price_res, None

                    if not current_price or current_price <= 0:
                        logger.warning(f"[PRICE_FAIL] {code}: 현재가 조회 실패 → 스킵")
                        continue

                    _update_last_price_memory(code, float(current_price), now_dt_kst.isoformat())

                    # === GOOD/BAD 타점 평가 ===
                    daily_ctx = _compute_daily_entry_context(
                        kis, code, current_price, price_source
                    )
                    daily_ctx = normalize_daily_ctx(daily_ctx)
                    intra_ctx = _compute_intraday_entry_context(
                        kis, code, fast=MOM_FAST, slow=MOM_SLOW
                    )
                    intra_ctx = normalize_intraday_ctx(intra_ctx)

                    momentum_confirmed = bool(
                        daily_ctx.get("strong_trend")
                        or intra_ctx.get("vwap_reclaim")
                        or intra_ctx.get("range_break")
                    )

                    if mode == "neutral" and not (
                        info.get("champion_grade") in ("A", "B") or momentum_confirmed
                    ):
                        logger.info(
                            f"[ENTRY-SKIP] {code}: neutral 레짐에서 비챔피언/모멘텀 미확인 → 신규 진입 보류"
                        )
                        continue

                    setup_state = signals.evaluate_setup_gate(
                        daily_ctx, intra_ctx, regime_state=regime_state
                    )
                    if not setup_state.get("ok"):
                        logger.info(
                            "[SETUP-BAD] %s | reasons=%s | daily=%s intra=%s regime=%s",
                            code,
                            setup_state.get("reasons"),
                            daily_ctx,
                            intra_ctx,
                            regime_state,
                        )
                        continue
                    logger.info(
                        "[SETUP-OK] %s | daily=%s intra=%s regime=%s",
                        code,
                        daily_ctx,
                        intra_ctx,
                        regime_state,
                    )

                    # === 전략별 진입 게이트 (전략 1~5) ===
                    strategy_id = _normalize_strategy_id(info.get("strategy_id") or _derive_strategy_id(info))
                    gate = strategy_entry_gate(
                        strategy_id,
                        info,
                        daily_ctx,
                        intra_ctx,
                        now_dt_kst=now_dt_kst,
                        regime_state=regime_state,
                    )
                    if not gate.get("ok"):
                        logger.info(
                            "[STRATEGY-GATE] code=%s sid=%s reasons=%s daily=%s intra=%s",
                            code,
                            strategy_id,
                            gate.get("reasons"),
                            daily_ctx,
                            intra_ctx,
                        )
                        continue

                    trigger_label = gate.get("trigger_label") or strategy_trigger_label(strategy_id, strategy)
                    trigger_state = signals.evaluate_trigger_gate(
                        daily_ctx,
                        intra_ctx,
                        prev_price=prev_price,
                        target_price=target_price,
                        trigger_name=trigger_label,
                    )
                    if not trigger_state.get("ok"):
                        logger.info(
                            "[TRIGGER-NO] %s | trigger=%s current=%s tgt_px=%s gap_pct=%s missing=%s signals=%s",
                            code,
                            trigger_state.get("trigger_name"),
                            trigger_state.get("current_price"),
                            trigger_state.get("target_price"),
                            trigger_state.get("gap_pct"),
                            trigger_state.get("missing_conditions"),
                            trigger_state.get("trigger_signals"),
                        )
                        continue
                    logger.info(
                        "[TRIGGER-OK] %s | trigger=%s current=%s tgt_px=%s gap_pct=%s signals=%s rr=%.2f",
                        code,
                        trigger_state.get("trigger_name"),
                        trigger_state.get("current_price"),
                        trigger_state.get("target_price"),
                        trigger_state.get("gap_pct"),
                        trigger_state.get("trigger_signals"),
                        trigger_state.get("risk_reward") or 0.0,
                    )

                    flow_ok, flow_ctx, ob_strength = _subject_flow_gate(
                        code,
                        info,
                        float(current_price),
                        target_price,
                        intraday_ctx.get("vwap"),
                    )
                    if not flow_ok:
                        continue

                    # === VWAP 가드(슬리피지 방어) ===
                    try:
                        guard_passed = vwap_guard(kis, code, SLIPPAGE_ENTER_GUARD_PCT)
                    except Exception as e:
                        logger.warning(
                            f"[VWAP_GUARD_FAIL] {code}: VWAP 가드 오류 → 진입 보류 ({e})"
                        )
                        continue

                    if not guard_passed:
                        logger.info(f"[VWAP_GUARD] {code}: 슬리피지 위험 → 매수 스킵")
                        continue

                    qty = target_qty
                    if mode == "neutral":
                        scaled_qty = max(1, int(qty * NEUTRAL_ENTRY_SCALE))
                        if scaled_qty < qty:
                            logger.info(
                                f"[ENTRY-SIZE] {code}: neutral 레짐 감축 {qty}→{scaled_qty} (스케일={NEUTRAL_ENTRY_SCALE})"
                            )
                        qty = scaled_qty

                    # 전략별 수량 스케일링 (예: 종가베팅은 리스크 축소)
                    try:
                        qty_scale = float(gate.get("qty_scale") or 1.0)
                    except Exception:
                        qty_scale = 1.0
                    if qty_scale and qty_scale != 1.0:
                        scaled_qty2 = max(1, int(qty * qty_scale))
                        if scaled_qty2 != qty:
                            logger.info(
                                "[ENTRY-SIZE][SID] %s: sid=%s qty %s→%s (scale=%.2f)",
                                code,
                                strategy_id,
                                qty,
                                scaled_qty2,
                                qty_scale,
                            )
                        qty = scaled_qty2
                    trade_ctx = {
                        "datetime": now_str,
                        "code": code,
                        "name": info.get("name"),
                        "qty": int(qty),
                        "K": k_value,
                        "target_price": target_price,
                        "strategy": strategy,
                        "strategy_id": info.get("strategy_id"),
                        "side": "BUY",
                    }

                    limit_px, mo_px = compute_entry_target(kis, info)
                    if limit_px is None and mo_px is None:
                        logger.warning(
                            f"[TARGET-PRICE] {code}: limit/mo 가격 산출 실패 → 스킵"
                        )
                        continue

                    if (
                        limit_px
                        and abs(limit_px - current_price) / current_price * 100
                        > SLIPPAGE_LIMIT_PCT
                    ):
                        logger.info(
                            f"[SLIPPAGE_LIMIT] {code}: 호가乖離 {abs(limit_px - current_price) / current_price * 100:.2f}% → 스킵"
                        )
                        continue

                    logger.info(
                        f"[BUY-TRY] {code}: qty={qty} limit={limit_px} mo={mo_px} target={target_price} k={k_value}"
                    )

                    prev_qty = int((holding.get(code) or {}).get("qty", 0))
                    if dry_run:
                        logger.info(
                            "[DRY-RUN][BUY] code=%s qty=%s price=%s strategy_id=%s",
                            code,
                            int(qty),
                            current_price,
                            strategy_id,
                        )
                        runtime_state_store.mark_order(
                            runtime_state,
                            code,
                            "BUY",
                            strategy_id,
                            int(qty),
                            float(current_price),
                            now_dt_kst.isoformat(),
                            status="submitted(dry)",
                        )
                        _save_runtime_state()
                        continue
                    result = place_buy_with_fallback(
                        kis, code, qty, limit_px or _round_to_tick(current_price)
                    )
                    runtime_state_store.mark_order(
                        runtime_state,
                        code,
                        "BUY",
                        strategy_id,
                        int(qty),
                        float(current_price),
                        now_dt_kst.isoformat(),
                        status="submitted",
                    )
                    _save_runtime_state()
                    if not _is_order_success(result):
                        logger.warning(f"[BUY-FAIL] {code}: result={result}")
                        continue

                    triggered_today.add(code)

                    exec_price = _extract_fill_price(result, current_price)
                    _record_trade(
                        traded,
                        code,
                        {
                            "buy_time": now_str,
                            "qty": int(qty),
                            "price": float(exec_price),
                            "status": "pending",
                            "pending_since": now_str,
                        },
                    )
                    traded_today.add(code)
                    save_state(holding, traded)
                    if not _is_balance_reflected(code, prev_qty=prev_qty):
                        logger.warning(
                            f"[BUY-PENDING] {code}: 잔고에 반영되지 않아 상태 기록 보류(result={result})"
                        )
                        continue
                    traded[code]["status"] = "filled"
                    _record_trade(
                        traded,
                        code,
                        {
                            "buy_time": now_str,
                            "qty": int(qty),
                            "price": float(exec_price),
                            "status": "filled",
                            "pending_since": None,
                        },
                    )
                    runtime_state_store.mark_fill(
                        runtime_state,
                        code,
                        "BUY",
                        strategy_id,
                        int(qty),
                        float(exec_price),
                        now_dt_kst.isoformat(),
                        status="filled",
                    )
                    _save_runtime_state()

                    _init_position_state(
                        kis,
                        holding,
                        code,
                        float(exec_price),
                        int(qty),
                        k_value,
                        target_price,
                    )
                    position_state = record_entry_state(
                        state=position_state,
                        code=code,
                        qty=int(qty),
                        avg_price=float(exec_price),
                        strategy_id=strategy_id,
                        engine=trigger_label,
                        entry_reason="SETUP-OK + TRIGGER-YES",
                        order_type="marketable_limit",
                        best_k=k_value,
                        tgt_px=target_price,
                        gap_pct_at_entry=trigger_state.get("gap_pct"),
                        entry_time=now_dt_kst.isoformat(),
                    )
                    position_state_dirty = True
                    _save_position_state_now()

                    lot_id = _build_lot_id(
                        result,
                        now_dt_kst.strftime("%Y%m%d%H%M%S%f"),
                        code,
                    )
                    before_lot_signature = _lot_state_signature(lot_state)
                    record_buy_fill(
                        lot_state,
                        lot_id=lot_id,
                        pdno=code,
                        strategy_id=strategy_id,
                        engine=f"legacy_kosdaq_runner:sid{strategy_id}",
                        entry_ts=now_dt_kst.isoformat(),
                        entry_price=float(exec_price),
                        qty=int(qty),
                        meta={
                            "strategy_name": strategy,
                            "entry_reason": str(gate.get("entry_reason") or "SETUP-OK") + " + TRIGGER-YES",
                            "strategy_gate": gate,
                            "k": k_value,
                            "target_price": target_price,
                            "best_k": k_value,
                            "tgt_px": target_price,
                            "engine": "legacy_kosdaq_runner",
                            "rebalance_date": str(rebalance_date),
                        },
                    )
                    logger.info(
                        "[LEDGER][BUY] code=%s sid=%s lot_id=%s qty=%s",
                        code,
                        strategy_id,
                        lot_id,
                        qty,
                    )
                    _maybe_save_lot_state(before_lot_signature)
                    if _lot_state_signature(lot_state) == before_lot_signature:
                        raise RuntimeError(
                            f"[LEDGER][BUY] failed to persist lot: code={code} sid={strategy_id}"
                        )

                    log_trade(
                        {
                            **trade_ctx,
                            "price": float(exec_price),
                            "amount": int(float(exec_price) * int(qty)),
                            "result": result,
                        }
                    )
                    effective_cash = _get_effective_ord_cash(
                        kis, soft_cap=effective_capital
                    )
                    if effective_cash <= 0:
                        can_buy = False
                    save_state(holding, traded)
                    time.sleep(RATE_SLEEP_SEC)

                # ====== 눌림목 전용 매수 (챔피언과 독립적으로 Top-N 시총 리스트 스캔) ======
                if USE_PULLBACK_ENTRY and is_open:
                    if not can_buy:
                        logger.info("[PULLBACK-SKIP] can_buy=False → 신규 매수 스킵")
                    else:
                        if pullback_watch:
                            logger.info(f"[PULLBACK-SCAN] {len(pullback_watch)}종목 검사")

                        for code, info in list(pullback_watch.items()):
                            if pullback_buys_today >= PULLBACK_MAX_BUYS_PER_DAY:
                                logger.info(
                                    f"[PULLBACK-LIMIT] 하루 최대 {PULLBACK_MAX_BUYS_PER_DAY}건 도달 → 스캔 중단"
                                )
                                break

                            if code in traded_today or code in holding:
                                continue  # 챔피언 루프와 별도로만 처리

                            if remaining_qty_for_strategy(lot_state, code, 5) > 0:
                                logger.info(
                                    "[ENTRY-SKIP] already owned in ledger: code=%s sid=5",
                                    code,
                                )
                                continue

                            if _pending_block(traded, code, now_dt_kst, block_sec=45):
                                logger.info(
                                    f"[PULLBACK-SKIP-PENDING] {code}: pending 쿨다운 중"
                                )
                                continue
                            if runtime_state_store.should_block_order(
                                runtime_state, code, "BUY", now_dt_kst.isoformat()
                            ):
                                logger.info(
                                    "[IDEMPOTENT-SKIP] %s BUY blocked within window",
                                    code,
                                )
                                continue

                            base_notional = int(info.get("notional") or 0)
                            if base_notional <= 0:
                                logger.info(f"[PULLBACK-SKIP] {code}: 예산 0")
                                continue

                            try:
                                resp = _detect_pullback_reversal(
                                    kis,
                                    code,
                                    lookback=PULLBACK_LOOKBACK,
                                    pullback_days=PULLBACK_DAYS,
                                    reversal_buffer_pct=PULLBACK_REVERSAL_BUFFER_PCT,
                                )

                                pullback_ok = False
                                trigger_price = None

                                if isinstance(resp, dict):
                                    pullback_ok = bool(resp.get("setup")) and bool(
                                        resp.get("reversing")
                                    )
                                    trigger_price = resp.get("reversal_price")
                                    if not pullback_ok:
                                        reason = resp.get("reason")
                                        if reason:
                                            logger.info(
                                                f"[PULLBACK-SKIP] {code}: 패턴 미충족(reason={reason})"
                                            )
                                elif isinstance(resp, tuple):
                                    if len(resp) >= 1:
                                        pullback_ok = bool(resp[0])
                                    if len(resp) >= 2:
                                        trigger_price = resp[1]
                                else:
                                    pullback_ok = bool(resp)

                            except Exception as e:
                                logger.warning(f"[PULLBACK-FAIL] {code}: 스캔 실패 {e}")
                                continue

                            if not pullback_ok:
                                continue

                            if trigger_price is None:
                                logger.info(f"[PULLBACK-SKIP] {code}: trigger_price None")
                                continue

                            qty = _notional_to_qty(kis, code, base_notional)
                            if qty <= 0:
                                logger.info(f"[PULLBACK-SKIP] {code}: 수량 산출 0")
                                continue

                            current_price = _safe_get_price(kis, code)
                            if not current_price:
                                logger.warning(f"[PULLBACK-PRICE] {code}: 현재가 조회 실패")
                                continue

                            if trigger_price and current_price < trigger_price * 0.98:
                                logger.info(
                                    f"[PULLBACK-DELAY] {code}: 가격이 트리거 대비 2% 이상 하락 → 대기 (cur={current_price}, trigger={trigger_price})"
                                )
                                continue

                            flow_ok, flow_ctx, ob_strength = _subject_flow_gate(
                                code,
                                info,
                                float(current_price),
                                trigger_price,
                                None,
                            )
                            if not flow_ok:
                                continue

                            prev_qty = int((holding.get(code) or {}).get("qty", 0))
                            if dry_run:
                                logger.info(
                                    "[DRY-RUN][BUY] code=%s qty=%s price=%s strategy_id=5",
                                    code,
                                    int(qty),
                                    trigger_price or current_price,
                                )
                                runtime_state_store.mark_order(
                                    runtime_state,
                                    code,
                                    "BUY",
                                    5,
                                    int(qty),
                                    float(trigger_price or current_price),
                                    now_dt_kst.isoformat(),
                                    status="submitted(dry)",
                                )
                                _save_runtime_state()
                                continue
                            result = place_buy_with_fallback(
                                kis,
                                code,
                                int(qty),
                                _round_to_tick(trigger_price or current_price),
                            )
                            runtime_state_store.mark_order(
                                runtime_state,
                                code,
                                "BUY",
                                5,
                                int(qty),
                                float(trigger_price or current_price),
                                now_dt_kst.isoformat(),
                                status="submitted",
                            )
                            _save_runtime_state()

                            if not _is_order_success(result):
                                logger.warning(
                                    f"[PULLBACK-BUY-FAIL] {code}: result={result}"
                                )
                                continue

                            triggered_today.add(code)
                            exec_price = _extract_fill_price(
                                result, trigger_price or current_price
                            )
                            _record_trade(
                                traded,
                                code,
                                {
                                    "buy_time": now_str,
                                    "qty": int(qty),
                                    "price": float(exec_price),
                                    "status": "pending",
                                    "pending_since": now_str,
                                },
                            )
                            traded_today.add(code)
                            save_state(holding, traded)
                            if not _is_balance_reflected(code, prev_qty=prev_qty):
                                logger.warning(
                                    f"[PULLBACK-PENDING] {code}: 잔고에 반영되지 않아 상태 기록 보류(result={result})"
                                )
                                continue

                            traded[code]["status"] = "filled"
                            holding[code] = {
                                "qty": int(qty),
                                "buy_price": float(exec_price),
                                "bear_s1_done": False,
                                "bear_s2_done": False,
                            }
                            _record_trade(
                                traded,
                                code,
                                {
                                    "buy_time": now_str,
                                    "qty": int(qty),
                                    "price": float(exec_price),
                                    "status": "filled",
                                    "pending_since": None,
                                },
                            )
                            runtime_state_store.mark_fill(
                                runtime_state,
                                code,
                                "BUY",
                                5,
                                int(qty),
                                float(exec_price),
                                now_dt_kst.isoformat(),
                                status="filled",
                            )
                            _save_runtime_state()
                            pullback_buys_today += 1

                            try:
                                _init_position_state(
                                    kis,
                                    holding,
                                    code,
                                    float(exec_price),
                                    int(qty),
                                    None,
                                    trigger_price,
                                )
                            except Exception as e:
                                logger.warning(f"[PULLBACK-INIT-FAIL] {code}: {e}")

                            pullback_meta = {}
                            if isinstance(resp, dict):
                                pullback_meta = {
                                    "pullback_peak_price": resp.get("peak_price"),
                                    "pullback_reversal_price": resp.get("reversal_price"),
                                    "pullback_reason": resp.get("reason"),
                                }
                            position_state = record_entry_state(
                                state=position_state,
                                code=code,
                                qty=int(qty),
                                avg_price=float(exec_price),
                                strategy_id=5,
                                engine="pullback",
                                entry_reason="PULLBACK-SETUP + REVERSAL",
                                order_type="marketable_limit",
                                best_k=None,
                                tgt_px=trigger_price,
                                gap_pct_at_entry=None,
                                meta=pullback_meta,
                                entry_time=now_dt_kst.isoformat(),
                            )
                            position_state_dirty = True
                            _save_position_state_now()

                            lot_id = _build_lot_id(
                                result,
                                now_dt_kst.strftime("%Y%m%d%H%M%S%f"),
                                code,
                            )
                            before_lot_signature = _lot_state_signature(lot_state)
                            record_buy_fill(
                                lot_state,
                                lot_id=lot_id,
                                pdno=code,
                                strategy_id=5,
                                engine=f"legacy_kosdaq_runner:sid{strategy_id}",
                                entry_ts=now_dt_kst.isoformat(),
                                entry_price=float(exec_price),
                                qty=int(qty),
                                meta={
                                    "strategy_name": f"코스닥 Top{PULLBACK_TOPN} 눌림목",
                                    "entry_reason": "PULLBACK-SETUP + REVERSAL",
                                    "k": None,
                                    "target_price": trigger_price,
                                    "best_k": None,
                                    "tgt_px": trigger_price,
                                    "pullback_peak_price": resp.get("peak_price")
                                    if isinstance(resp, dict)
                                    else None,
                                    "pullback_reversal_price": resp.get("reversal_price")
                                    if isinstance(resp, dict)
                                    else None,
                                    "engine": "legacy_kosdaq_runner",
                                    "rebalance_date": str(rebalance_date),
                                },
                            )
                            logger.info(
                                "[LEDGER][BUY] code=%s sid=%s lot_id=%s qty=%s",
                                code,
                                5,
                                lot_id,
                                qty,
                            )
                            _maybe_save_lot_state(before_lot_signature)
                            if _lot_state_signature(lot_state) == before_lot_signature:
                                raise RuntimeError(
                                    f"[LEDGER][BUY] failed to persist lot: code={code} sid=5"
                                )

                            logger.info(
                                f"[✅ 눌림목 매수] {code}, qty={qty}, price={exec_price}, trigger={trigger_price}, result={result}"
                            )

                            log_trade(
                                {
                                    "datetime": now_str,
                                    "code": code,
                                    "name": info.get("name"),
                                    "qty": int(qty),
                                    "K": None,
                                    "target_price": trigger_price,
                                    "strategy": f"코스닥 Top{PULLBACK_TOPN} 눌림목",
                                    "strategy_id": 5,
                                    "side": "BUY",
                                    "price": float(exec_price),
                                    "amount": int(float(exec_price) * int(qty)),
                                    "result": result,
                                }
                            )
                            effective_cash = _get_effective_ord_cash(
                                kis, soft_cap=effective_capital
                            )
                            if effective_cash <= 0:
                                can_buy = False
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)

                # ====== (A) 비타겟 보유분도 장중 능동관리 ======
                if is_open:
                    for code in list(holding.keys()):
                        if code in code_to_target:
                            continue  # 위 루프에서 이미 처리

                        # 약세 단계 축소(비타겟)
                        if regime["mode"] == "bear":
                            _run_bear_reduction(code, is_target=False, regime=regime)

                        try:
                            momentum_intact, trend_ctx = _has_bullish_trend_structure(
                                kis, code
                            )
                        except NetTemporaryError:
                            logger.warning(
                                f"[20D_TREND_TEMP_SKIP] {code}: 네트워크 일시 실패 → 이번 루프 스킵"
                            )
                            continue
                        except DataEmptyError:
                            logger.warning(
                                f"[DATA_EMPTY] {code}: 0캔들 → 다음 루프에서 재확인"
                            )
                            continue
                        except DataShortError:
                            logger.error(
                                f"[DATA_SHORT] {code}: 21개 미만 → 이번 루프 판단 스킵"
                            )
                            continue

                        if momentum_intact:
                            logger.info(
                                (
                                    f"[모멘텀 보유] {code}: 5/10/20 정배열 & 20일선 상승 & 종가>20일선 유지 "
                                    f"(close={trend_ctx.get('last_close'):.2f}, ma5={trend_ctx.get('ma5'):.2f}, "
                                    f"ma10={trend_ctx.get('ma10'):.2f}, ma20={trend_ctx.get('ma20'):.2f}→{trend_ctx.get('ma20_prev'):.2f})"
                                )
                            )
                            continue

                if regime_s1_summary.get("by_stock"):
                    logger.info(
                        f"[REGIME_S1][SUMMARY] sent_qty={regime_s1_summary['sent_qty']} "
                        f"sent_orders={regime_s1_summary['sent_orders']} "
                        f"skipped={regime_s1_summary['skipped']} total_qty={regime_s1_summary['total_qty']} "
                        f"by_stock={regime_s1_summary['by_stock']}"
                    )

                # --- 장중 커트오프(KST): 14:40 도달 시 "전량매도 없이" 리포트 생성 후 정상 종료 ---
                if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
                    logger.info(
                        f"[⏰ 커트오프] {SELL_FORCE_TIME.strftime('%H:%M')} 도달: 전량 매도 없이 리포트 생성 후 종료"
                    )

                    save_state(holding, traded)
                    if position_state_dirty:
                        save_position_state(position_state_path, position_state)
                        position_state_dirty = False

                    try:
                        _report = ceo_report(datetime.now(KST), period="daily")
                        logger.info(
                            f"[📄 CEO Report 생성 완료] title={_report.get('title')}"
                        )
                    except Exception as e:
                        logger.error(f"[CEO Report 생성 실패] {e}")

                    logger.info("[✅ 커트오프 완료: 루프 정상 종료]")
                    break

                save_state(holding, traded)
                if position_state_dirty:
                    save_position_state(position_state_path, position_state)
                    position_state_dirty = False
                time.sleep(loop_sleep_sec)

        except KeyboardInterrupt:
            logger.info("[🛑 수동 종료]")
        except Exception as e:
            logger.exception(f"[FATAL] 메인 루프 예외 발생: {e}")


if __name__ == "__main__":
    main()
