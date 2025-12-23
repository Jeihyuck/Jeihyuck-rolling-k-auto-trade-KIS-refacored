# -*- coding: utf-8 -*-
"""Subject flow wrapper with caching/fallback so trader never halts."""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional, Tuple

from trader.config import (
    SUBJECT_FLOW_CACHE_TTL_SEC,
    SUBJECT_FLOW_DEGRADED_OB_ADD,
    SUBJECT_FLOW_DEGRADED_TURNOVER_MULT,
    SUBJECT_FLOW_EMPTY_POLICY,
    SUBJECT_FLOW_FAIL_POLICY,
    SUBJECT_FLOW_MAX_CALLS_PER_RUN,
)
from trader.metrics import smart_money_score, subject_flow_gate
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)

_FLOW_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_FLOW_CACHE_LOCK = threading.Lock()
_FLOW_CALL_COUNT = 0


def reset_flow_call_count() -> None:
    """Reset per-run subject flow call counter."""
    global _FLOW_CALL_COUNT
    with _FLOW_CACHE_LOCK:
        _FLOW_CALL_COUNT = 0


def _safe_num(val: Any) -> int:
    try:
        if val is None:
            return 0
        if isinstance(val, (int, float)):
            return int(val)
        return int(str(val).replace(",", ""))
    except Exception:
        return 0


def _cache_get(key: Tuple[str, str]) -> Optional[Dict[str, Any]]:
    with _FLOW_CACHE_LOCK:
        payload = _FLOW_CACHE.get(key)
    if not payload:
        return None
    if time.time() - float(payload.get("ts", 0)) > SUBJECT_FLOW_CACHE_TTL_SEC:
        return None
    return payload


def _cache_set(key: Tuple[str, str], value: Dict[str, Any]) -> None:
    payload = dict(value)
    payload["ts"] = time.time()
    with _FLOW_CACHE_LOCK:
        _FLOW_CACHE[key] = payload


def _increment_call_count() -> int:
    global _FLOW_CALL_COUNT
    with _FLOW_CACHE_LOCK:
        _FLOW_CALL_COUNT += 1
        return _FLOW_CALL_COUNT


def _current_call_count() -> int:
    with _FLOW_CACHE_LOCK:
        return _FLOW_CALL_COUNT


def _log_flow(
    *,
    code: str,
    market: str,
    ok: bool,
    used: str,
    policy: str,
    score: Dict[str, Any] | None,
    turnover: float,
    degraded: bool,
    error: str | None,
) -> None:
    score = score or {}
    logger.info(
        "[FLOW] code=%s market=%s ok=%s used=%s policy=%s smart_money_krw=%s ratio=%.6f "
        "orgn=%s frgn=%s prsn=%s turnover=%.0f degraded=%s error=%s",
        code,
        market,
        ok,
        used,
        policy,
        score.get("smart_money_krw"),
        float(score.get("smart_money_ratio") or 0.0),
        score.get("orgn"),
        score.get("frgn"),
        score.get("prsn"),
        float(turnover or 0.0),
        degraded,
        error,
    )


def get_subject_flow_with_fallback(
    kis: Any,
    code: str,
    market: str,
    day_turnover_krw: float,
) -> Dict[str, Any]:
    """Fetch investor flow with cache/fallback; never raises."""
    policy = SUBJECT_FLOW_FAIL_POLICY.upper()
    empty_policy = SUBJECT_FLOW_EMPTY_POLICY.upper()
    used = "none"
    degraded = False
    score: Dict[str, Any] | None = None
    inv: Dict[str, Any] | None = None
    error: str | None = None
    flow_ok = False
    decision = "UNCLASSIFIED"
    turnover_guard_mult = 1.0
    ob_strength_add = 0.0

    try:
        today = now_kst().strftime("%Y%m%d")
        key = (today, code)
        cached = _cache_get(key)
        if cached:
            used = "cache"
            inv = cached.get("inv")
            score = cached.get("score")
            if cached.get("ok") and score:
                flow_ok = subject_flow_gate(market, float(score.get("smart_money_ratio") or 0))
                decision = "CACHE_OK"
                _log_flow(
                    code=code,
                    market=market,
                    ok=flow_ok,
                    used=used,
                    policy=policy,
                    score=score,
                    turnover=day_turnover_krw,
                    degraded=False,
                    error=None,
                )
                return {
                    "flow_ok": flow_ok,
                    "decision": decision,
                    "used": used,
                    "policy": policy,
                    "score": score,
                    "inv": inv,
                    "degraded": degraded,
                    "turnover_guard_mult": turnover_guard_mult,
                    "ob_strength_add": ob_strength_add,
                    "cache_hit": True,
                }

        if _current_call_count() >= SUBJECT_FLOW_MAX_CALLS_PER_RUN:
            error = "CALL_LIMIT"
            policy = "CACHE"
        else:
            _increment_call_count()
            resp: Dict[str, Any] = kis.inquire_investor(code, market)
            used = "live"
            if resp.get("ok"):
                inv = resp.get("inv") or {}
                orgn = _safe_num(inv.get("orgn_ntby_tr_pbmn"))
                frgn = _safe_num(inv.get("frgn_ntby_tr_pbmn"))
                prsn = _safe_num(inv.get("prsn_ntby_tr_pbmn"))
                inv.update({
                    "orgn_ntby_tr_pbmn": orgn,
                    "frgn_ntby_tr_pbmn": frgn,
                    "prsn_ntby_tr_pbmn": prsn,
                })
                if empty_policy == "TREAT_AS_FAIL" and (orgn == 0 and frgn == 0 and prsn == 0):
                    error = "EMPTY_FLOW"
                else:
                    score = smart_money_score(inv, day_turnover_krw)
                    flow_ok = subject_flow_gate(market, score["smart_money_ratio"])
                    decision = "LIVE_OK"
                    _cache_set(key, {"ok": True, "inv": inv, "score": score})
            else:
                error = str(resp.get("error") or "UNCLASSIFIED_ERROR")

        if score and used == "live":
            _log_flow(
                code=code,
                market=market,
                ok=flow_ok,
                used=used,
                policy=policy,
                score=score,
                turnover=day_turnover_krw,
                degraded=False,
                error=None,
            )
            return {
                "flow_ok": flow_ok,
                "decision": decision,
                "used": used,
                "policy": policy,
                "score": score,
                "inv": inv,
                "degraded": degraded,
                "turnover_guard_mult": turnover_guard_mult,
                "ob_strength_add": ob_strength_add,
                "cache_hit": False,
            }

        cache_after_fail = cached if cached else _cache_get(key)
        if error and policy == "CACHE" and cache_after_fail:
            used = "cache"
            inv = cache_after_fail.get("inv")
            score = cache_after_fail.get("score")
            flow_ok = subject_flow_gate(market, float(score.get("smart_money_ratio") or 0)) if score else False
            decision = "CACHE_ON_FAIL"
        elif policy == "PASS":
            flow_ok = True
            decision = "PASS_NO_FLOW"
        elif policy == "DEGRADED":
            flow_ok = True
            degraded = True
            turnover_guard_mult = SUBJECT_FLOW_DEGRADED_TURNOVER_MULT
            ob_strength_add = SUBJECT_FLOW_DEGRADED_OB_ADD
            decision = "PASS_DEGRADED"
        else:  # BLOCK or CACHE without cache
            flow_ok = False
            decision = "BLOCK_NO_FLOW"

        _log_flow(
            code=code,
            market=market,
            ok=flow_ok,
            used=used,
            policy=policy,
            score=score,
            turnover=day_turnover_krw,
            degraded=degraded,
            error=error,
        )
        return {
            "flow_ok": flow_ok,
            "decision": decision,
            "used": used,
            "policy": policy,
            "score": score,
            "inv": inv,
            "degraded": degraded,
            "turnover_guard_mult": turnover_guard_mult,
            "ob_strength_add": ob_strength_add,
            "cache_hit": cached is not None,
            "error": error,
        }

    except Exception as e:  # noqa: BLE001 - 최상위 안전망
        logger.exception("[FLOW-FAILSAFE] %s(%s) 예외: %s", code, market, e)
        _log_flow(
            code=code,
            market=market,
            ok=False,
            used=used,
            policy=policy,
            score=score,
            turnover=day_turnover_krw,
            degraded=False,
            error=str(e),
        )
        return {
            "flow_ok": False,
            "decision": "EXCEPTION",
            "used": used,
            "policy": policy,
            "score": score,
            "inv": inv,
            "degraded": False,
            "turnover_guard_mult": 1.0,
            "ob_strength_add": 0.0,
            "cache_hit": False,
            "error": str(e),
        }
