"""포지션 초기화, 체결, 레짐 관련 기능."""
from __future__ import annotations

import logging

import csv
import json
import os
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from .core_constants import *  # noqa: F401,F403
from .config import KST, STATE_PATH
from .code_utils import normalize_code
from .core_utils import (
    _get_daily_candles_cached,
    _log_realized_pnl,
    _round_to_tick,
    _to_float,
    _to_int,
    _with_retry,
    log_trade,
)
from .kis_wrapper import KisAPI, NetTemporaryError
from .fills import append_fill
from .signals import (
    _get_atr,
    _notional_to_qty,
    _safe_get_price,
    _weight_to_qty,
    get_20d_return_pct,
    is_strong_momentum,
    _detect_pullback_reversal,
)
from .metrics import vwap_guard

__all__ = [
    "fetch_rebalancing_targets",
    "_init_position_state",
    "_init_position_state_from_balance",
    "_maybe_scale_in_dips",
    "_sell_once",
    "ensure_fill_has_name",
    "compute_entry_target",
    "place_buy_with_fallback",
    "_get_kosdaq_snapshot",
    "_update_market_regime",
    "log_champion_and_regime",
    "_adaptive_exit",
    "REGIME_STATE",
    "record_entry_state",
    "update_position_meta",
    "update_position_flags",
]


def _normalize_entry_meta(
    *,
    code: str,
    strategy_id: Any,
    engine: str,
    entry_reason: str,
    order_type: str | None,
    best_k: Any,
    tgt_px: Any,
    gap_pct_at_entry: Any,
    entry_time: str | None = None,
) -> Dict[str, Any]:
    return {
        "time": entry_time or datetime.now(KST).isoformat(),
        "strategy_id": strategy_id,
        "engine": engine,
        "entry_reason": entry_reason,
        "order_type": order_type,
        "best_k": best_k,
        "tgt_px": tgt_px,
        "gap_pct_at_entry": gap_pct_at_entry,
    }


def _normalize_meta(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = payload or {}
    return {
        "pullback_peak_price": payload.get("pullback_peak_price"),
        "pullback_reversal_price": payload.get("pullback_reversal_price"),
        "pullback_reason": payload.get("pullback_reason"),
    }


def _normalize_flags(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = payload or {}
    return {
        "bear_s1_done": bool(payload.get("bear_s1_done", False)),
        "bear_s2_done": bool(payload.get("bear_s2_done", False)),
    }


def record_entry_state(
    *,
    state: Dict[str, Any],
    code: str,
    qty: int,
    avg_price: float,
    strategy_id: Any,
    engine: str,
    entry_reason: str,
    order_type: str | None,
    best_k: Any,
    tgt_px: Any,
    gap_pct_at_entry: Any,
    meta: Dict[str, Any] | None = None,
    flags: Dict[str, Any] | None = None,
    entry_time: str | None = None,
) -> Dict[str, Any]:
    code_key = normalize_code(code)
    sid_key = str(strategy_id)
    pos = state.setdefault("positions", {}).setdefault(
        code_key,
        {
            "strategies": {},
        },
    )
    strategies = pos.setdefault("strategies", {})
    existing = strategies.get(sid_key)
    if not isinstance(existing, dict):
        entry_flags = {
            "bear_s1_done": False,
            "bear_s2_done": False,
            "sold_p1": False,
            "sold_p2": False,
        }
        if flags:
            entry_flags.update(
                {k: bool(flags.get(k)) for k in entry_flags.keys() if k in flags}
            )
        entry_meta = _normalize_meta(meta)
        entry_meta.setdefault("high", float(avg_price))
        entry_meta["high"] = max(float(entry_meta.get("high") or 0.0), float(avg_price))
        now_ts = entry_time or datetime.now(KST).isoformat()
        strategies[sid_key] = {
            "qty": int(qty),
            "avg_price": float(avg_price),
            "entry": _normalize_entry_meta(
                code=str(code),
                strategy_id=strategy_id,
                engine=engine,
                entry_reason=entry_reason,
                order_type=order_type,
                best_k=best_k,
                tgt_px=tgt_px,
                gap_pct_at_entry=gap_pct_at_entry,
                entry_time=now_ts,
            ),
            "meta": entry_meta,
            "flags": entry_flags,
            "code": code_key,
            "sid": str(strategy_id),
            "engine": engine,
            "entry_ts": now_ts,
            "high_watermark": float(entry_meta.get("high") or avg_price),
            "last_update_ts": now_ts,
        }
    else:
        prev_qty = int(existing.get("qty") or 0)
        add_qty = int(qty)
        total_qty = prev_qty + add_qty
        prev_avg = float(existing.get("avg_price") or 0.0)
        new_avg = (
            (prev_avg * prev_qty + float(avg_price) * add_qty) / total_qty
            if total_qty > 0
            else 0.0
        )
        existing["qty"] = int(total_qty)
        existing["avg_price"] = float(new_avg)
        entry = existing.setdefault("entry", {})
        entry_time_value = entry_time or datetime.now(KST).isoformat()
        entry["last_entry_time"] = entry_time_value
        entry["strategy_id"] = entry.get("strategy_id") or str(strategy_id)
        entry_meta = existing.setdefault("meta", {})
        if not entry_meta.get("high") or float(entry_meta.get("high") or 0.0) <= 0:
            entry_meta["high"] = float(new_avg)
        entry_meta["high"] = max(float(entry_meta.get("high") or 0.0), float(new_avg))
        existing["code"] = code_key
        existing["sid"] = str(strategy_id)
        existing["engine"] = engine
        existing["entry_ts"] = entry.get("time") or entry_time_value
        existing["high_watermark"] = max(
            float(existing.get("high_watermark") or 0.0),
            float(entry_meta.get("high") or 0.0),
            float(new_avg),
        )
        existing["last_update_ts"] = entry_time_value
        entry_flags = existing.setdefault(
            "flags",
            {
                "bear_s1_done": False,
                "bear_s2_done": False,
                "sold_p1": False,
                "sold_p2": False,
            },
        )
        if flags:
            for key, value in flags.items():
                if key in entry_flags:
                    entry_flags[key] = bool(value)
    logger.info(
        "[ENTRY] code=%s strategy=%s engine=%s best_k=%s tgt_px=%s saved_state=OK",
        code_key,
        strategy_id,
        engine,
        best_k,
        tgt_px,
    )
    return state


def update_position_meta(
    state: Dict[str, Any],
    code: str,
    strategy_id: Any,
    meta_updates: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    code_key = normalize_code(code)
    sid_key = str(strategy_id)
    pos = state.get("positions", {}).get(code_key)
    if not isinstance(pos, dict):
        return state
    strategies = pos.get("strategies", {})
    entry = strategies.get(sid_key)
    if not isinstance(entry, dict):
        return state
    meta = entry.setdefault(
        "meta",
        {
            "pullback_peak_price": None,
            "pullback_reversal_price": None,
            "pullback_reason": None,
        },
    )
    if meta_updates:
        for key in ("pullback_peak_price", "pullback_reversal_price", "pullback_reason"):
            if key in meta_updates:
                meta[key] = meta_updates.get(key)
    entry["last_update_ts"] = datetime.now(KST).isoformat()
    return state


def update_position_flags(
    state: Dict[str, Any],
    code: str,
    strategy_id: Any,
    flag_updates: Dict[str, Any],
) -> Dict[str, Any]:
    assert strategy_id is not None, "strategy_id required for update_position_flags"
    code_key = normalize_code(code)
    pos = state.get("positions", {}).get(code_key)
    if not isinstance(pos, dict):
        return state
    strategies = pos.setdefault("strategies", {})
    entry = strategies.get(str(strategy_id))
    if not isinstance(entry, dict):
        return state
    flags = entry.setdefault(
        "flags",
        {"bear_s1_done": False, "bear_s2_done": False, "sold_p1": False, "sold_p2": False},
    )
    before_flags = dict(flags)
    for key in ("bear_s1_done", "bear_s2_done", "sold_p1", "sold_p2"):
        if key in flag_updates:
            flags[key] = bool(flag_updates.get(key))
    logger.info(
        "[FLAGS] code=%s flags_before=%s flags_after=%s",
        code_key,
        before_flags,
        flags,
    )
    entry["last_update_ts"] = datetime.now(KST).isoformat()
    return state

def fetch_rebalancing_targets(date: str) -> list[dict[str, Any]]:
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}?force_order=true"
    response = requests.post(REBALANCE_API_URL)
    logger.info(f"[🛰️ 리밸런싱 API 전체 응답]: {response.text}")
    if response.status_code == 200:
        data = response.json()
        selected = data.get("selected") or data.get("selected_stocks") or []
        logger.info(f"[🎯 리밸런싱 종목]: {selected}")
        try:
            champion = selected[0] if selected else None
            log_champion_and_regime(logger, champion, REGIME_STATE, context="rebalance_api")
        except Exception as e:
            logger.exception(f"[VWAP_CHAMPION_LOG_ERROR] {e}")
        return selected
    raise Exception(f"리밸런싱 API 호출 실패: {response.text}")

def _init_position_state(kis: KisAPI, holding: Dict[str, Any], code: str, entry_price: float, qty: int, k_value: Any, target_price: Optional[float]) -> None:
    code = normalize_code(code)
    try:
        _ = kis.is_market_open()
    except Exception:
        pass
    atr = _get_atr(kis, code)
    rng_eff = (atr * 1.5) if (atr and atr > 0) else max(1.0, entry_price * 0.01)
    t1 = entry_price + 0.5 * rng_eff
    t2 = entry_price + 1.0 * rng_eff
    holding[code] = {
        'qty': int(qty),
        'buy_price': float(entry_price),
        'entry_time': datetime.now(KST).isoformat(),
        'high': float(entry_price),
        'tp1': float(t1),
        'tp2': float(t2),
        'sold_p1': False,
        'sold_p2': False,
        'trail_pct': TRAIL_PCT,
        'atr': float(atr) if atr else None,
        'stop_abs': float(entry_price - ATR_STOP * atr) if atr else float(entry_price * (1 - FAST_STOP)),
        'k_value': k_value,
        'target_price_src': float(target_price) if target_price is not None else None,
        'bear_s1_done': False,
        'bear_s2_done': False,
        # 눌림목 3단계 진입 관련 기본값 (신규 매수 직후 overwrite 가능)
        'entry_stage': 1,
        'max_price_after_entry': float(entry_price),
        'planned_total_qty': int(qty),
        'stage1_qty': int(qty),
        'stage2_qty': 0,
        'stage3_qty': 0,
    }

def _init_position_state_from_balance(kis: KisAPI, holding: Dict[str, Any], code: str, avg_price: float, qty: int) -> None:
    code = normalize_code(code)
    if qty <= 0 or code in holding:
        return
    try:
        _ = kis.is_market_open()
    except Exception:
        pass
    atr = _get_atr(kis, code)
    rng_eff = (atr * 1.5) if (atr and atr > 0) else max(1.0, avg_price * 0.01)
    t1 = avg_price + 0.5 * rng_eff
    t2 = avg_price + 1.0 * rng_eff
    holding[code] = {
        'qty': int(qty),
        'buy_price': float(avg_price),
        'entry_time': (datetime.now(KST) - timedelta(minutes=10)).isoformat(),
        'high': float(avg_price),
        'tp1': float(t1),
        'tp2': float(t2),
        'sold_p1': False,
        'sold_p2': False,
        'trail_pct': TRAIL_PCT,
        'atr': float(atr) if atr else None,
        'stop_abs': float(avg_price - ATR_STOP * atr) if atr else float(avg_price * (1 - FAST_STOP)),
        'k_value': None,
        'target_price_src': None,
        'bear_s1_done': False,
        'bear_s2_done': False,
        # 기존 보유분은 추가 진입(stage 3 완료 상태)으로 간주
        'entry_stage': 3,
        'max_price_after_entry': float(avg_price),
        'planned_total_qty': int(qty),
        'stage1_qty': int(qty),
        'stage2_qty': 0,
        'stage3_qty': 0,
    }


def _maybe_scale_in_dips(
    kis: KisAPI,
    holding: Dict[str, Any],
    code: str,
    target: Dict[str, Any],
    now_str: str,
    regime_mode: str,
    position_state: Dict[str, Any] | None = None,
) -> None:
    """
    신고가 → 3일 연속 하락 → 반등 확인 시 단계적 추가 매수 로직.
    - entry_stage: 1 → 2차 진입 후보(반등 확인선 돌파), 2 → 3차 진입 후보(신고가 회복)
    - bull / neutral 모드에서만 동작, bear 모드에서는 추가 진입 금지
    """
    code_key = normalize_code(code)
    if not code_key:
        return
    pos = holding.get(code_key)
    if not pos:
        return

    # 약세 레짐에서는 추가 진입 금지
    if regime_mode not in ("bull", "neutral"):
        return

    entry_stage = int(pos.get("entry_stage") or 1)
    if entry_stage >= 3:
        return

    # 현재가 조회
    try:
        cur_price = _safe_get_price(kis, code_key)
    except Exception:
        cur_price = None
    if cur_price is None or cur_price <= 0:
        return

    # 손절선 이하면 추가 진입 금지
    try:
        stop_abs = pos.get("stop_abs")
        if stop_abs is not None and cur_price <= float(stop_abs):
            logger.info(
                f"[SCALE-IN-GUARD] {code_key}: 현재가({cur_price}) <= stop_abs({stop_abs}) → 추가 진입 금지"
            )
            return
    except Exception:
        pass

    # VWAP 가드: 과도한 추세 붕괴 구간에서는 추가 진입하지 않음
    try:
        vwap_val = kis.get_vwap_today(code_key)
    except Exception:
        vwap_val = None
    if vwap_val is None or vwap_val <= 0:
        logger.debug(f"[SCALE-IN-VWAP-SKIP] {code_key}: VWAP 데이터 없음 → VWAP 가드 생략")
    else:
        if not vwap_guard(float(cur_price), float(vwap_val), VWAP_TOL):
            logger.info(
                f"[SCALE-IN-VWAP-GUARD] {code_key}: 현재가({cur_price}) < VWAP*(1 - {VWAP_TOL:.4f}) "
                f"→ 눌림목 추가 진입 스킵 (VWAP={vwap_val:.2f})"
            )
            return

    # 계획 수량 계산
    planned_total_qty = int(
        pos.get("planned_total_qty")
        or _to_int(target.get("매수수량") or target.get("qty"), 0)
    )
    if planned_total_qty <= 0:
        return

    # 스테이지별 목표 수량(부족 시 재계산)
    s1 = int(pos.get("stage1_qty") or max(1, int(planned_total_qty * ENTRY_LADDERS[0])))
    s2 = int(pos.get("stage2_qty") or max(0, int(planned_total_qty * ENTRY_LADDERS[1])))
    s3 = int(pos.get("stage3_qty") or max(0, planned_total_qty - s1 - s2))

    pos["planned_total_qty"] = int(planned_total_qty)
    pos["stage1_qty"] = int(s1)
    pos["stage2_qty"] = int(s2)
    pos["stage3_qty"] = int(s3)

    current_qty = int(pos.get("qty") or 0)
    if current_qty <= 0:
        return

    # 신고가 → 3일 눌림 → 반등 여부 확인
    pullback = _detect_pullback_reversal(
        kis=kis,
        code=code_key,
        current_price=float(cur_price),
    )
    if USE_PULLBACK_ENTRY and not pullback.get("setup"):
        logger.info(
            f"[PULLBACK-SKIP] {code_key}: 신고가 눌림 패턴 미충족 → reason={pullback.get('reason')}"
        )
        return

    if USE_PULLBACK_ENTRY and not pullback.get("reversing"):
        rev_px = pullback.get("reversal_price")
        logger.info(
            f"[PULLBACK-WAIT] {code_key}: 현재가({cur_price}) < 반등확인선({rev_px}) → 대기"
        )
        return

    reversal_price = pullback.get("reversal_price") or float(cur_price)
    peak_price = pullback.get("peak_price") or reversal_price

    # 참고용 상태 업데이트
    pos["pullback_peak_price"] = float(peak_price)
    pos["pullback_reversal_price"] = float(reversal_price)
    if position_state is not None:
        update_position_meta(
            position_state,
            code_key,
            pos.get("strategy_id") or 1,
            {
                "pullback_peak_price": float(peak_price),
                "pullback_reversal_price": float(reversal_price),
                "pullback_reason": pullback.get("reason"),
            },
        )

    add_qty = 0
    next_stage = entry_stage

    if entry_stage == 1:
        # 2차 진입: 3일 눌림 후 반등 확인선 돌파 → s1+s2까지 확대
        if cur_price >= reversal_price and current_qty < (s1 + s2):
            add_qty = max(0, (s1 + s2) - current_qty)
            next_stage = 2
    elif entry_stage == 2:
        # 3차 진입: 신고가 회복(peak_price 돌파) 시 전체 planned_total_qty까지 확대
        if cur_price >= peak_price and current_qty < planned_total_qty:
            add_qty = max(0, planned_total_qty - current_qty)
            next_stage = 3
    else:
        return

    if add_qty <= 0:
        return

    logger.info(
        f"[SCALE-IN] {code} stage={entry_stage}->{next_stage} "
        f"reversal_line={reversal_price:.2f} peak={peak_price:.2f} cur={cur_price} add_qty={add_qty}"
    )

    # 추가 매수 실행 (현재가 기준 가드형 지정가/시장가)
    try:
        result = place_buy_with_fallback(
            kis, code, int(add_qty), limit_price=int(cur_price)
        )
    except Exception as e:
        logger.error(f"[SCALE-IN-ORDER-FAIL] {code}: {e}")
        return

    # fills CSV 보강
    try:
        odno = ""
        if isinstance(result, dict):
            out = result.get("output") or {}
            odno = (
                out.get("ODNO")
                or out.get("ord_no")
                or out.get("order_no")
                or ""
            )
        ensure_fill_has_name(
            odno=odno,
            code=code,
            name=str(target.get("name") or target.get("종목명") or ""),
            qty=int(add_qty),
            price=float(cur_price),
        )
    except Exception as e:
        logger.warning(f"[SCALE-IN-FILL-NAME-FAIL] code={code} ex={e}")

    # 상태 업데이트
    pos["qty"] = int(current_qty + add_qty)
    pos["entry_stage"] = int(next_stage)
    holding[code] = pos

    # 매수 로그 기록
    try:
        log_trade(
            {
                "datetime": now_str,
                "code": code,
                "name": target.get("name") or target.get("종목명"),
                "qty": int(add_qty),
                "K": pos.get("k_value"),
                "target_price": pos.get("target_price_src"),
                "strategy": "눌림목 3단계 진입",
                "side": "BUY",
                "price": float(cur_price),
                "amount": int(float(cur_price)) * int(add_qty),
                "result": result,
                "reason": f"scale_in_stage_{entry_stage}_to_{next_stage}",
            }
        )
    except Exception as e:
        logger.warning(f"[SCALE-IN-LOG-FAIL] {code}: {e}")


def _sell_once(kis: KisAPI, code: str, qty: int, prefer_market=True) -> Tuple[Optional[float], Any]:
    code = normalize_code(code)
    cur_price = _safe_get_price(kis, code)
    try:
        if prefer_market and hasattr(kis, "sell_stock_market"):
            result = _with_retry(kis.sell_stock_market, code, qty)
        else:
            result = _with_retry(kis.sell_stock, code, qty)
    except Exception as e:
        logger.warning(f"[매도 재시도: 토큰 갱신 후 1회] {code} qty={qty} err={e}")
        try:
            if hasattr(kis, "refresh_token"):
                kis.refresh_token()
        except Exception:
            pass
        if prefer_market and hasattr(kis, "sell_stock_market"):
            result = _with_retry(kis.sell_stock_market, code, qty)
        else:
            result = _with_retry(kis.sell_stock, code, qty)
    logger.info(f"[매도호출] {code}, qty={qty}, price(log)={cur_price}, result={result}")
    return cur_price, result

def ensure_fill_has_name(odno: str, code: str, name: str, qty: int = 0, price: float = 0.0) -> None:
    code = normalize_code(code)
    try:
        fills_dir = Path("fills")
        fills_dir.mkdir(exist_ok=True)
        today_path = fills_dir / f"fills_{datetime.now().strftime('%Y%m%d')}.csv"
        updated = False
        if today_path.exists():
            with open(today_path, "r", encoding="utf-8", newline="") as f:
                reader = list(csv.reader(f))
            if reader:
                header = reader[0]
                try:
                    idx_odno = header.index("ODNO")
                    idx_code = header.index("code")
                    idx_name = header.index("name")
                except ValueError:
                    idx_odno = None
                    idx_code = None
                    idx_name = None
                if idx_odno is not None and idx_name is not None and idx_code is not None:
                    for i in range(1, len(reader)):
                        row = reader[i]
                        if len(row) <= max(idx_odno, idx_code, idx_name):
                            continue
                        if (row[idx_odno] == str(odno) or (not row[idx_odno] and str(odno) == "")) and row[idx_code] == str(code):
                            if not row[idx_name]:
                                row[idx_name] = name or ""
                                reader[i] = row
                                updated = True
                                logger.info(f"[FILL_NAME_UPDATE] ODNO={odno} code={code} name={name}")
                                break
        if updated:
            with open(today_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(reader)
            return
        append_fill(
            "BUY",
            code,
            name or "",
            qty,
            price or 0.0,
            odno or "",
            note="ensure_fill_added_by_trader",
            reason="ensure_fill_name",
        )
    except Exception as e:
        logger.warning(f"[ENSURE_FILL_FAIL] odno={odno} code={code} ex={e}")

# === 앵커: 목표가 계산 함수 ===
def compute_entry_target(kis: KisAPI, stk: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    code = normalize_code(stk.get("code") or stk.get("stock_code") or stk.get("pdno") or "")
    if not code:
        return None, None

    try:
        market_open = kis.is_market_open()
    except Exception:
        market_open = True

    # 1) 오늘 시초가
    today_open = None
    try:
        today_open = kis.get_today_open(code)
    except Exception:
        pass
    if not today_open or today_open <= 0:
        try:
            snap = kis.get_current_price(code)
            if snap and snap > 0:
                today_open = float(snap)
        except Exception:
            pass
    if not today_open or today_open <= 0:
        logger.info(f"[TARGET/wait_open] {code} 오늘 시초가 미확정 → 목표가 계산 보류")
        return None, None

    # 2) 전일 범위
    prev_high = prev_low = None
    try:
        if market_open:
            prev_candles = _get_daily_candles_cached(kis, code, count=2)
            if prev_candles and len(prev_candles) >= 2:
                prev = prev_candles[-2]
                prev_high = _to_float(prev.get("high"))
                prev_low  = _to_float(prev.get("low"))
    except Exception:
        pass

    if prev_high is None or prev_low is None:
        try:
            prev_candles = _get_daily_candles_cached(kis, code, count=2)
            if prev_candles and len(prev_candles) >= 2:
                prev = prev_candles[-2]
                prev_high = _to_float(prev.get("high"))
                prev_low  = _to_float(prev.get("low"))
        except Exception:
            pass

    if prev_high is None or prev_low is None:
        prev_high = _to_float(stk.get("prev_high"))
        prev_low  = _to_float(stk.get("prev_low"))
        if prev_high is None or prev_low is None:
            logger.warning(f"[TARGET/prev_candle_fail] {code} 전일 캔들/백업 모두 부재")
            return None, None

    rng = max(0.0, float(prev_high) - float(prev_low))
    k_used = float(stk.get("best_k") or stk.get("K") or stk.get("k") or 0.5)
    raw_target = float(today_open) + rng * k_used

    eff_target_price = float(_round_to_tick(raw_target, mode="up"))
    return float(eff_target_price), float(k_used)

def place_buy_with_fallback(kis: KisAPI, code: str, qty: int, limit_price: int) -> Dict[str, Any]:
    """
    매수 주문(지정가 우선, 실패시 시장가 Fallback) + 체결가/슬리피지/네트워크 장애/실패 상세 로깅
    """
    code = normalize_code(code)
    result_limit: Optional[Dict[str, Any]] = None
    order_price = _round_to_tick(limit_price, mode="up") if (limit_price and limit_price > 0) else 0
    fill_price = None
    trade_logged = False

    try:
        # [PATCH] 예수금/과매수 방지: 가드형 지정가 사용
        if hasattr(kis, "buy_stock_limit_guarded") and order_price and order_price > 0:  # [PATCH]
            result_limit = _with_retry(kis.buy_stock_limit_guarded, code, qty, int(order_price))  # [PATCH]
            logger.info("[BUY-LIMIT] %s qty=%s limit=%s -> %s", code, qty, order_price, result_limit)
            time.sleep(2.0)
            filled = False
            if hasattr(kis, "check_filled"):
                try:
                    filled = bool(_with_retry(kis.check_filled, result_limit))
                except Exception:
                    filled = False
            if filled:
                try:
                    fill_price = float(result_limit.get("output", {}).get("prdt_price", 0)) or None
                except Exception:
                    fill_price = None
                if fill_price is None:
                    try:
                        fill_price = kis.get_current_price(code)
                    except Exception:
                        fill_price = None
                slippage = ((fill_price - order_price) / order_price * 100.0) if (fill_price and order_price) else None
                log_trade({
                    "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "code": code,
                    "side": "BUY",
                    "order_price": order_price,
                    "fill_price": fill_price,
                    "slippage_pct": round(slippage, 2) if slippage is not None else None,
                    "qty": qty,
                    "result": result_limit,
                    "status": "filled",
                    "fail_reason": None
                })
                trade_logged = True
                if slippage is not None and abs(slippage) > SLIPPAGE_LIMIT_PCT:
                    logger.warning(f"[슬리피지 경고] {code} slippage {slippage:.2f}% > 임계값({SLIPPAGE_LIMIT_PCT}%)")
                return result_limit
        else:
            logger.info("[BUY-LIMIT] API 미지원 또는 limit_price 무효 → 시장가로 진행")
    except Exception as e:
        logger.error("[BUY-LIMIT-FAIL] %s qty=%s limit=%s err=%s", code, qty, order_price, e)
        log_trade({
            "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "code": code,
            "side": "BUY",
            "order_price": order_price,
            "fill_price": None,
            "slippage_pct": None,
            "qty": qty,
            "result": None,
            "status": "failed",
            "fail_reason": str(e)
        })
        trade_logged = True

    # --- 시장가 Fallback ---
    try:
        # [PATCH] 예수금/과매수 방지: 가드형 시장가 사용
        if hasattr(kis, "buy_stock_market_guarded"):  # [PATCH]
            result_mkt = _with_retry(kis.buy_stock_market_guarded, code, qty)  # [PATCH]
        elif hasattr(kis, "buy_stock_market"):
            result_mkt = _with_retry(kis.buy_stock_market, code, qty)
        else:
            result_mkt = _with_retry(kis.buy_stock, code, qty)
        logger.info("[BUY-MKT] %s qty=%s (from limit=%s) -> %s", code, qty, order_price, result_mkt)
        try:
            fill_price = float(result_mkt.get("output", {}).get("prdt_price", 0)) or None
        except Exception:
            fill_price = None
        if fill_price is None:
            try:
                fill_price = kis.get_current_price(code)
            except Exception:
                fill_price = None
        slippage = ((fill_price - order_price) / order_price * 100.0) if (fill_price and order_price) else None
        log_trade({
            "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "code": code,
            "side": "BUY",
            "order_price": order_price or None,
            "fill_price": fill_price,
            "slippage_pct": round(slippage, 2) if slippage is not None else None,
            "qty": qty,
            "result": result_mkt,
            "status": "filled" if result_mkt and result_mkt.get("rt_cd") == "0" else "failed",
            "fail_reason": None if result_mkt and result_mkt.get("rt_cd") == "0" else "체결실패"
        })
        trade_logged = True
        if slippage is not None and abs(slippage) > SLIPPAGE_LIMIT_PCT:
            logger.warning(f"[슬리피지 경고] {code} slippage {slippage:.2f}% > 임계값({SLIPPAGE_LIMIT_PCT}%)")
        return result_mkt
    except Exception as e:
        logger.error("[BUY-MKT-FAIL] %s qty=%s err=%s", code, qty, e)
        if not trade_logged:
            log_trade({
                "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "side": "BUY",
                "order_price": order_price or None,
                "fill_price": None,
                "slippage_pct": None,
                "qty": qty,
                "result": None,
                "status": "failed",
                "fail_reason": str(e)
            })
        raise

# === [ANCHOR: REGIME PARAMS] 코스닥 레짐 파라미터 ===
REGIME_ENABLED = True
KOSDAQ_CODE = _cfg("KOSDAQ_INDEX_CODE")
KOSDAQ_ETF_FALLBACK = _cfg("KOSDAQ_ETF_FALLBACK")  # KODEX 코스닥150

REG_BULL_MIN_UP_PCT = float(_cfg("REG_BULL_MIN_UP_PCT"))
REG_BULL_MIN_MINUTES = int(_cfg("REG_BULL_MIN_MINUTES"))
REG_BEAR_VWAP_MINUTES = int(_cfg("REG_BEAR_VWAP_MINUTES"))
REG_BEAR_DROP_FROM_HIGH = float(_cfg("REG_BEAR_DROP_FROM_HIGH"))

REG_BEAR_STAGE1_MINUTES = int(_cfg("REG_BEAR_STAGE1_MINUTES"))
REG_BEAR_STAGE2_ADD_DROP = float(_cfg("REG_BEAR_STAGE2_ADD_DROP"))
REG_PARTIAL_S1 = float(_cfg("REG_PARTIAL_S1"))
REG_PARTIAL_S2 = float(_cfg("REG_PARTIAL_S2"))

TRAIL_PCT_BULL = float(_cfg("TRAIL_PCT_BULL"))
TRAIL_PCT_BEAR = float(_cfg("TRAIL_PCT_BEAR"))
TP_PROFIT_PCT_BASE = DEFAULT_PROFIT_PCT
TP_PROFIT_PCT_BULL = float(_cfg("TP_PROFIT_PCT_BULL"))

# === [ANCHOR: REGIME STATE] 코스닥 레짐 상태 ===
REGIME_STATE: Dict[str, Any] = {
    "mode": "neutral",          # 'bull' | 'bear' | 'neutral'
    "since": None,              # regime 시작 시각(datetime)
    "bear_stage": 0,            # 0/1/2
    "session_high": None,       # 당일 코스닥 고점
    "last_above_vwap_ts": None, # 최근 VWAP 상방 유지 시작시각
    "last_below_vwap_ts": None, # 최근 VWAP 하방 유지 시작시각
    "last_snapshot_ts": None,   # 최근 스냅샷 시간
    "vwap": None,               # 가능하면 채움
    "prev_close": None,         # 전일 종가
    "pct_change": None,          # 등락률(%)
    "stage": 0,
    "R20": None,
    "D1": None
}

# === [ANCHOR: REGIME TABLES] 레짐별 자본 스케일 / 최대 보유 종목 수 / 챔피언 비중 ===
# mode ∈ {'bull','bear','neutral'}, stage ∈ {0,1,2}
REGIME_CAPITAL_SCALE: Dict[Tuple[str, int], float] = {
    ("bull", 2): 1.00,
    ("bull", 1): 0.75,
    ("neutral", 0): 0.50,
    ("bear", 1): 0.30,
    ("bear", 2): 0.15,
}

REGIME_MAX_ACTIVE: Dict[Tuple[str, int], int] = {
    ("bull", 2): 7,
    ("bull", 1): 5,
    ("neutral", 0): 3,
    ("bear", 1): 2,
    ("bear", 2): 1,
}

# 순위별 비중 (합계 1.0 기준)
REGIME_WEIGHTS: Dict[Tuple[str, int], List[float]] = {
    ("bull", 2): [0.25, 0.18, 0.15, 0.13, 0.11, 0.09, 0.09],
    ("bull", 1): [0.28, 0.22, 0.18, 0.17, 0.15],
    ("neutral", 0): [0.40, 0.35, 0.25],
    ("bear", 1): [0.60, 0.40],
    ("bear", 2): [1.00],
}

# 각 종목 Target Notional 내에서 3단계 눌림목 진입 비중
ENTRY_LADDERS: List[float] = [0.40, 0.35, 0.25]

def _get_kosdaq_snapshot(kis: KisAPI) -> Dict[str, Optional[float]]:
    """
    코스닥 지수 스냅샷. 래퍼에 인덱스 조회가 없으면 ETF(229200)로 근사.
    반환: {'price', 'prev_close', 'pct_change', 'vwap', 'above_vwap'}
    """
    price = prev_close = vwap = None

    # 1) 인덱스 시도
    try:
        if hasattr(kis, "get_index_quote"):
            q = kis.get_index_quote(KOSDAQ_CODE)
            if isinstance(q, dict):
                price = _to_float(q.get("price"))
                prev_close = _to_float(q.get("prev_close"))
                vwap = _to_float(q.get("vwap"))
    except Exception:
        pass

    # 2) 폴백: ETF로 근사
    if price is None or prev_close is None:
        try:
            etf = KOSDAQ_ETF_FALLBACK
            last = _to_float(kis.get_current_price(etf))
            cs = kis.get_daily_candles(etf, count=2)
            pc = _to_float(cs[-2]['close']) if cs and len(cs) >= 2 and 'close' in cs[-2] else None
            if last and pc:
                price, prev_close = last, pc
                vwap = None
        except Exception:
            pass

    pct_change = None
    try:
        if price and prev_close and prev_close > 0:
            pct_change = (price - prev_close) / prev_close * 100.0
    except Exception:
        pct_change = None

    above_vwap = None
    try:
        if price is not None and vwap:
            above_vwap = bool(price >= vwap)
    except Exception:
        above_vwap = None

    return {"price": price, "prev_close": prev_close, "pct_change": pct_change, "vwap": vwap, "above_vwap": above_vwap}


def _update_market_regime(kis: KisAPI) -> Dict[str, Any]:
    """코스닥 지수 20일 수익률(R20) + 당일 수익률(D1) 기반 레짐 판정.

    - R20, D1은 KOSDAQ 지수 또는 ETF(KOSDAQ_ETF_FALLBACK)의 일봉으로 계산
    - 레짐(mode, stage) 규칙

      * bull-2:  R20 ≥ +6%  AND D1 ≥ +2.5%
      * bull-1:  R20 ≥ +3%  AND D1 ≥ +0.5%  (단, bull-2는 제외)
      * bear-2:  R20 ≤ -6%  AND D1 ≤ -2.5%
      * bear-1:  R20 ≤ -3%  AND D1 ≤ -0.5%  (단, bear-2는 제외)
      * neutral: -3% < R20 < +3%
                 또는 (|R20| ≥ 3% 이지만 D1이 -0.5% ~ +0.5% 사이인 흔들리는 날)

    stage:
      * bull: 1/2
      * bear: 1/2
      * neutral: 0
    """
    if not REGIME_ENABLED:
        return REGIME_STATE

    now = datetime.now(KST)

    # 스냅샷(전일 종가, 일중 등락률) 업데이트
    snap = _get_kosdaq_snapshot(kis)
    REGIME_STATE["last_snapshot_ts"] = now
    REGIME_STATE["prev_close"] = snap.get("prev_close")
    REGIME_STATE["pct_change"] = snap.get("pct_change")

    # R20 / D1 계산 (기본: KOSDAQ ETF 일봉)
    R20 = None
    D1 = None
    try:
        etf = KOSDAQ_ETF_FALLBACK
        candles = kis.get_daily_candles(etf, count=21)
        if candles and len(candles) >= 21:
            # candles는 과거→현재 순서로 정렬되어 있음
            close_20ago = float(candles[0]["close"])
            close_yday = float(candles[-2]["close"])
            close_today = float(candles[-1]["close"])
            if close_20ago > 0 and close_yday > 0:
                R20 = (close_today / close_20ago - 1.0) * 100.0
                D1 = (close_today / close_yday - 1.0) * 100.0
    except Exception as e:
        logger.warning(f"[REGIME] R20/D1 계산 실패: {e}")

    REGIME_STATE["R20"] = R20
    REGIME_STATE["D1"] = D1

    mode = REGIME_STATE.get("mode") or "neutral"
    stage = int(REGIME_STATE.get("stage") or 0)

    if R20 is None or D1 is None:
        # 데이터가 없으면 보수적으로 neutral-0
        mode, stage = "neutral", 0
    else:
        # 우선순위: 강한 강세/약세 → 일반 강세/약세 → 중립
        if R20 >= 6.0 and D1 >= 2.5:
            mode, stage = "bull", 2
        elif R20 >= 3.0 and D1 >= 0.5:
            mode, stage = "bull", 1
        elif R20 <= -6.0 and D1 <= -2.5:
            mode, stage = "bear", 2
        elif R20 <= -3.0 and D1 <= -0.5:
            mode, stage = "bear", 1
        elif (-3.0 < R20 < 3.0) or (abs(R20) >= 3.0 and -0.5 <= D1 <= 0.5):
            mode, stage = "neutral", 0
        else:
            # 나머지 애매한 케이스는 보수적으로 neutral-0 처리
            mode, stage = "neutral", 0

    REGIME_STATE["mode"] = mode
    REGIME_STATE["stage"] = stage
    # 기존 bear_stage는 약세일 때만 stage를 반영(하위 로직 호환용)
    REGIME_STATE["bear_stage"] = stage if mode == "bear" else 0

    return REGIME_STATE

def log_champion_and_regime(
    logger: logging.Logger,
    champion,
    regime_state: Dict[str, Any],
    context: Any,   # ✅ str -> Any 로 변경
) -> None:
    try:
        now_kst = datetime.now(KST)
        now_str = now_kst.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ✅ dict로 들어오면 JSON 문자열로 변환해서 로그에 보기 좋게 찍기
    if isinstance(context, dict):
        try:
            context_label = json.dumps(context, ensure_ascii=False, sort_keys=True)
        except Exception:
            context_label = str(context)
    else:
        context_label = str(context)

    # 1) 챔피언 종목 선정 사유(최소한 코드/이름/스코어 등 기본 정보 위주)
    if champion is None:
        logger.info(
            "[VWAP_CHAMPION] %s | %s | champion=None (선택된 종목이 없습니다.)",
            now_str,
            context,
        )
    else:
        # champion 형식이 문자열(종목코드)인지, dict인지 모두 처리
        if isinstance(champion, str):
            code = champion
            name = "-"
            detail = "rebalance_api selected[0] 기준 챔피언"
        elif isinstance(champion, dict):
            code = champion.get("code") or champion.get("symbol") or champion.get("stock_code") or "?"
            name = champion.get("name") or champion.get("stock_name") or champion.get("nm") or "?"

            # 메타-K 리밸런싱 결과에 실제로 존재하는 필드들 위주로 사유 구성
            best_k = champion.get("best_k")
            avg_ret = champion.get("avg_return_pct")
            win = champion.get("win_rate_pct")
            mdd = champion.get("mdd_pct")
            cumret = champion.get("cumulative_return_pct")
            trades = champion.get("trades")
            sharpe_m = champion.get("sharpe_m")
            tgt = champion.get("target_price") or champion.get("목표가")
            close = champion.get("close")
            turnover = champion.get("prev_turnover")

            detail_parts = []

            if best_k is not None:
                detail_parts.append(f"best_k={best_k}")
            if avg_ret is not None:
                detail_parts.append(f"avg_ret={avg_ret}%")
            if win is not None:
                detail_parts.append(f"winrate={win}%")
            if mdd is not None:
                detail_parts.append(f"mdd={mdd}%")
            if cumret is not None:
                detail_parts.append(f"cumret={cumret}%")
            if trades is not None:
                detail_parts.append(f"trades={trades}")
            if sharpe_m is not None:
                detail_parts.append(f"sharpe_m={sharpe_m}")
            if tgt is not None and close is not None:
                # 목표가/현재가 차이도 한 줄로 요약
                try:
                    gap_pct = (tgt - close) / close * 100.0
                    detail_parts.append(f"target={tgt}, close={close}, gap={gap_pct:.2f}%")
                except Exception:
                    detail_parts.append(f"target={tgt}, close={close}")
            if turnover is not None:
                detail_parts.append(f"prev_turnover={turnover}")

            detail = ", ".join(detail_parts) if detail_parts else "meta-K 백테스트 기반 정보 없음"

        else:
            code = str(champion)
            name = "-"
            detail = "알 수 없는 champion 타입"

        logger.info(
            "[VWAP_CHAMPION] %s | %s | code=%s, name=%s, detail=%s",
            now_str,
            context,
            code,
            name,
            detail,
        )

    # 2) 레짐 상태 상세 로그
    if regime_state:
        logger.info(
            "[VWAP_REGIME] %s | %s | mode=%s, score=%s, kosdaq_ret5=%s, drop_stage=%s, since=%s, comment=%s",
            now_str,
            context,
            regime_state.get("mode"),
            regime_state.get("score"),
            regime_state.get("kosdaq_ret5"),
            regime_state.get("bear_stage"),
            regime_state.get("since"),
            regime_state.get("comment"),
        )

def _adaptive_exit(
    kis: KisAPI,
    code: str,
    pos: Dict[str, Any],
    regime_mode: str = "neutral",
) -> Tuple[Optional[str], Optional[int]]:
    """
    레짐(강세/약세/중립) + 1분봉 모멘텀 기반
    - 부분 익절(1차/2차)
    - 트레일링 스탑
    - 손절
    을 동적으로 적용하는 매도 엔진.
    한 번 호출에서 "한 번의 매도"만 실행하고, 그 결과만 반환한다.
    """
    now = datetime.now(KST)
    reason: Optional[str] = None

    # 현재가 조회
    try:
        cur = _safe_get_price(kis, code)
        if cur is None or cur <= 0:
            logger.warning(f"[EXIT-FAIL] {code} 현재가 조회 실패")
            return None, None
    except Exception as e:
        logger.error(f"[EXIT-FAIL] {code} 현재가 조회 예외: {e}")
        return None, None

    # === 상태/기초 값 ===
    qty = _to_int(pos.get("qty"), 0)
    if qty <= 0:
        logger.warning(f"[EXIT-FAIL] {code} qty<=0")
        return None, None

    buy_price = float(pos.get("buy_price", 0.0)) or 0.0
    if buy_price <= 0:
        logger.warning(f"[EXIT-FAIL] {code} buy_price<=0")
        return None, None

    # 최고가(high) 갱신
    pos["high"] = max(float(pos.get("high", cur)), float(cur))
    max_price = float(pos["high"])

    # 현재 누적 수익률
    pnl_pct = (cur - buy_price) / buy_price * 100.0

    # 부분 익절 플래그 & 비율
    sold_p1 = bool(pos.get("sold_p1", False))
    sold_p2 = bool(pos.get("sold_p2", False))
    qty_p1 = max(1, int(qty * PARTIAL1))
    qty_p2 = max(1, int(qty * PARTIAL2))

    # === 레짐 기반 TP/트레일링 설정 ===
    base_tp1 = DEFAULT_PROFIT_PCT        # 보통 3.0
    base_tp2 = DEFAULT_PROFIT_PCT * 2    # 6.0
    trail_down_frac = 0.018              # 기본: 고점대비 1.8% 되돌리면 컷

    # (선택) 모멘텀 정보를 쓰고 싶으면 여기서 strong_mom 계산
    strong_mom = False
    try:
        # metrics에 is_strong_momentum이 있다면 사용, 없으면 False 유지
        strong_mom = bool(is_strong_momentum(kis, code))
    except Exception:
        strong_mom = False

    if regime_mode == "bull":
        # 좋은 장: 기본 목표 상향
        tp1 = base_tp1 + 1.0      # 4%
        tp2 = base_tp2 + 2.0      # 8%
        trail_down_frac = 0.025   # 2.5%

        if strong_mom:
            # 장도 좋고 모멘텀도 강하면 한 번 더 상향
            tp1 += 1.0            # 5%
            tp2 += 2.0            # 10%
            trail_down_frac = 0.03

    elif regime_mode == "neutral":
        tp1 = base_tp1            # 3%
        tp2 = base_tp2            # 6%
        trail_down_frac = 0.018

        if strong_mom:
            tp1 = base_tp1 + 1.0  # 4%
            tp2 = base_tp2 + 2.0  # 8%
            trail_down_frac = 0.02

    elif regime_mode == "bear":
        # 약세장: 보수적으로
        tp1 = 2.0
        tp2 = 4.0
        trail_down_frac = 0.01
    else:
        tp1 = base_tp1
        tp2 = base_tp2
        trail_down_frac = 0.018

    # 손절 기준
    hard_stop_pct = DEFAULT_LOSS_PCT

    sell_size: int = 0

    # === 1) 손절 ===
    if pnl_pct <= hard_stop_pct:
        reason = f"손절 {hard_stop_pct:.1f}%"
        sell_size = qty

    # === 2) 2차 TP (더 높은 수익 구간) ===
    elif (pnl_pct >= tp2) and (not sold_p2) and qty > 1:
        reason = f"2차 익절 {tp2:.1f}%"
        sell_size = min(qty, qty_p2)
        pos["sold_p2"] = True

    # === 3) 1차 TP ===
    elif (pnl_pct >= tp1) and (not sold_p1) and qty > 1:
        reason = f"1차 익절 {tp1:.1f}%"
        sell_size = min(qty, qty_p1)
        pos["sold_p1"] = True

    else:
        # === 4) 트레일링 스탑 ===
        if max_price >= buy_price * (1 + tp1 / 100.0) and cur <= max_price * (1 - trail_down_frac):
            reason = f"트레일링스톱({trail_down_frac*100:.1f}%)"
            sell_size = qty
        else:
            # 청산 조건 없음 → 보유 유지
            return None, None

    return reason, sell_size
