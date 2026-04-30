"""trader/execution_guard.py

AM/PM 공통 Buyable Guard — 모든 종목·모든 세션에 적용.

역할
----
entry 후보가 실제 주문 후보로 올라가기 전에, 공통 차단 규칙을 적용한다.

차단 조건
---------
1. BUYABLE_BLOCK_TODAY_EXIT_EXISTS   — 당일 STOP 계열 매도 주문/체결 발생
2. BUYABLE_BLOCK_EXIT_COOLDOWN       — 같은 거래일 exit block (PB1_BLOCK_REENTRY_AFTER_EXIT_SAME_DAY=1)
3. BUYABLE_BLOCK_POSITION_EXISTS     — 보유 포지션 qty > 0
4. BUYABLE_BLOCK_OPEN_ORDER_EXISTS   — 미체결/접수 주문 존재
5. BUYABLE_BLOCK_TODAY_BUY_EXISTS    — 당일 BUY 주문(어떤 상태든) 이미 존재
6. BUYABLE_BLOCK_PENDING_FILL_CONFIRM — 주문 접수 후 체결미확인 상태
7. BUYABLE_OK                        — 모든 조건 통과

이 모듈은 특정 종목코드·종목명에 무관하게 동일 로직을 적용한다.
hardcoding된 종목코드/종목명이 절대 없어야 한다.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# STOP 계열 exit reason 목록 (이 이유로 매도되면 당일 재진입 금지)
# ---------------------------------------------------------------------------
STOP_EXIT_REASONS: frozenset[str] = frozenset(
    {
        "STOP_HIT_EFFECTIVE",
        "EXIT_HARD_STOP",
        "STOP_LOSS",
        "TRAILING_STOP",
        "EXIT_TRAILING_STOP",
        "FAILED_BREAKOUT_EXIT",
        "RISK_OFF_EXIT",
        "EXIT_RISK_OFF",
        "MA20_BREAK_EXIT",
        "MA50_BREAK_EXIT",
        "EXIT_MA20_BREAK",
        "EXIT_MA50_BREAK",
        "TIME_STOP_EXIT",
        "EXIT_TIME_STOP",
        "MANUAL_RISK_EXIT",
        "EXIT_SWING_TIME_STOP",
        "EXIT_SWING_RUNNER_MA20_BREAK",
        "EXIT_CORE_HARD_STOP",
        "EXIT_CORE_RISK_OFF",
        "EXIT_CORE_MA50_BREAK",
        "EXIT_DAY_STOP_LOSS",
    }
)

# 매도 상태 중 "주문이 살아있다"고 볼 수 있는 상태
ACTIVE_SELL_STATUSES: frozenset[str] = frozenset(
    {
        "INTENT",
        "BUILT",
        "SUBMITTED",
        "ACCEPTED",
        "PENDING_CONFIRM",
        "PARTIALLY_FILLED",
        "FILLED",
    }
)

# 매수 중복 차단 대상 상태
ACTIVE_BUY_STATUSES: frozenset[str] = frozenset(
    {
        "INTENT",
        "BUILT",
        "SUBMITTED",
        "ACCEPTED",
        "PENDING_CONFIRM",
        "PARTIALLY_FILLED",
        "FILLED",
    }
)


@dataclass
class BuyableResult:
    """Buyable Guard 판단 결과."""

    code: str
    session_kind: str
    trade_date: str
    buyable: bool
    blocked_reason: Optional[str] = None
    blocked_details: Dict[str, Any] = field(default_factory=dict)


def _get_env_flag(name: str, default: str = "1") -> bool:
    return os.getenv(name, default) not in {"0", "false", "False", "no", "NO"}


def _normalize_reason(reason: Any) -> str:
    return str(reason or "").strip().upper()


def is_symbol_buyable_today(
    *,
    code: str,
    session_kind: str,
    trade_date_kst: str,
    current_positions: List[Dict[str, Any]],
    open_orders: List[Dict[str, Any]],
    today_orders: List[Dict[str, Any]],
    today_fills: List[Dict[str, Any]],
    today_exit_events: List[Dict[str, Any]],
    allow_pyramiding: bool = False,
) -> BuyableResult:
    """모든 종목·세션에 공통으로 적용하는 buyable 판단 함수.

    특정 종목코드/종목명 조건 절대 금지.

    Parameters
    ----------
    code : str
        6자리 종목코드 (zfill(6) 후 전달)
    session_kind : str
        "am" | "afternoon" | "close"
    trade_date_kst : str
        "YYYY-MM-DD" KST 기준 거래일
    current_positions : list
        현재 보유 포지션 목록 (code, qty 필드 필요)
    open_orders : list
        미체결/접수 주문 목록 (code, side, status 필드 필요)
    today_orders : list
        당일 주문 목록 (code, side, status, exit_reason 필드 필요)
    today_fills : list
        당일 체결 목록 (code, side, exit_reason 필드 필요)
    today_exit_events : list
        당일 exit 이벤트 목록 (code, exit_reason, status 필드 필요)
    allow_pyramiding : bool
        피라미딩 허용 여부 (True이면 position_exists 체크 건너뜀)
    """
    code_norm = str(code or "").zfill(6)
    block_reentry = _get_env_flag("PB1_BLOCK_REENTRY_AFTER_EXIT_SAME_DAY", "1")
    allow_intraday_reentry = _get_env_flag("PB1_ALLOW_INTRADAY_REENTRY_AFTER_STOP", "0")

    # -------------------------------------------------------------------------
    # 1. 당일 STOP 계열 매도 → 재진입 금지
    # -------------------------------------------------------------------------
    if block_reentry and not allow_intraday_reentry:
        last_exit_reason = _check_today_exit_block(
            code=code_norm,
            today_orders=today_orders,
            today_fills=today_fills,
            today_exit_events=today_exit_events,
        )
        if last_exit_reason is not None:
            logger.info(
                "[BUYABLE][GUARD] session=%s code=%s buyable=0 "
                "reason=BUYABLE_BLOCK_TODAY_EXIT_EXISTS last_exit_reason=%s trade_date=%s",
                session_kind,
                code_norm,
                last_exit_reason,
                trade_date_kst,
            )
            logger.info(
                "[BUYABLE][BLOCK] session=%s code=%s reason=BUYABLE_BLOCK_EXIT_COOLDOWN "
                "last_exit_reason=%s until=next_trading_day",
                session_kind,
                code_norm,
                last_exit_reason,
            )
            return BuyableResult(
                code=code_norm,
                session_kind=session_kind,
                trade_date=trade_date_kst,
                buyable=False,
                blocked_reason="BUYABLE_BLOCK_EXIT_COOLDOWN",
                blocked_details={
                    "last_exit_reason": last_exit_reason,
                    "trade_date": trade_date_kst,
                    "until": "next_trading_day",
                },
            )

    # -------------------------------------------------------------------------
    # 2. 보유 포지션 존재 → 신규 매수 금지 (피라미딩 비활성화 시)
    # -------------------------------------------------------------------------
    if not allow_pyramiding:
        pos_qty = _get_position_qty(code=code_norm, current_positions=current_positions)
        if pos_qty > 0:
            logger.info(
                "[BUYABLE][GUARD] session=%s code=%s buyable=0 "
                "reason=BUYABLE_BLOCK_POSITION_EXISTS qty=%s",
                session_kind,
                code_norm,
                pos_qty,
            )
            return BuyableResult(
                code=code_norm,
                session_kind=session_kind,
                trade_date=trade_date_kst,
                buyable=False,
                blocked_reason="BUYABLE_BLOCK_POSITION_EXISTS",
                blocked_details={"qty": pos_qty},
            )

    # -------------------------------------------------------------------------
    # 3. 미체결/접수 BUY 또는 SELL 주문 존재 → 추가 매수 금지
    # -------------------------------------------------------------------------
    open_order = _find_open_order(code=code_norm, open_orders=open_orders)
    if open_order is not None:
        logger.info(
            "[BUYABLE][GUARD] session=%s code=%s buyable=0 "
            "reason=BUYABLE_BLOCK_OPEN_ORDER_EXISTS side=%s status=%s",
            session_kind,
            code_norm,
            open_order.get("side"),
            open_order.get("status"),
        )
        return BuyableResult(
            code=code_norm,
            session_kind=session_kind,
            trade_date=trade_date_kst,
            buyable=False,
            blocked_reason="BUYABLE_BLOCK_OPEN_ORDER_EXISTS",
            blocked_details={
                "side": open_order.get("side"),
                "status": open_order.get("status"),
                "order_id": open_order.get("order_id") or open_order.get("id"),
            },
        )

    # -------------------------------------------------------------------------
    # 4. 당일 BUY 주문 이미 존재 (상태 무관)
    # -------------------------------------------------------------------------
    today_buy_order = _find_today_buy_order(
        code=code_norm, today_orders=today_orders, today_fills=today_fills
    )
    if today_buy_order is not None:
        order_status = today_buy_order.get("status")
        logger.info(
            "[BUYABLE][GUARD] session=%s code=%s buyable=0 "
            "reason=BUYABLE_BLOCK_TODAY_BUY_EXISTS order_status=%s",
            session_kind,
            code_norm,
            order_status,
        )
        return BuyableResult(
            code=code_norm,
            session_kind=session_kind,
            trade_date=trade_date_kst,
            buyable=False,
            blocked_reason="BUYABLE_BLOCK_TODAY_BUY_EXISTS",
            blocked_details={
                "order_status": order_status,
                "order_id": today_buy_order.get("order_id") or today_buy_order.get("id"),
            },
        )

    # -------------------------------------------------------------------------
    # 5. 주문 접수 후 체결 미확인 (PENDING_CONFIRM)
    # -------------------------------------------------------------------------
    pending = _find_pending_fill_confirm(code=code_norm, today_orders=today_orders)
    if pending is not None:
        logger.info(
            "[BUYABLE][GUARD] session=%s code=%s buyable=0 "
            "reason=BUYABLE_BLOCK_PENDING_FILL_CONFIRM status=%s",
            session_kind,
            code_norm,
            pending.get("status"),
        )
        return BuyableResult(
            code=code_norm,
            session_kind=session_kind,
            trade_date=trade_date_kst,
            buyable=False,
            blocked_reason="BUYABLE_BLOCK_PENDING_FILL_CONFIRM",
            blocked_details={
                "status": pending.get("status"),
                "order_id": pending.get("order_id") or pending.get("id"),
            },
        )

    logger.info(
        "[BUYABLE][GUARD] session=%s code=%s buyable=1 reason=BUYABLE_OK trade_date=%s",
        session_kind,
        code_norm,
        trade_date_kst,
    )
    return BuyableResult(
        code=code_norm,
        session_kind=session_kind,
        trade_date=trade_date_kst,
        buyable=True,
        blocked_reason=None,
        blocked_details={},
    )


def apply_buyable_guard_for_candidates(
    *,
    candidates: List[Dict[str, Any]],
    session_kind: str,
    trade_date_kst: str,
    current_positions: List[Dict[str, Any]],
    open_orders: List[Dict[str, Any]],
    today_orders: List[Dict[str, Any]],
    today_fills: List[Dict[str, Any]],
    today_exit_events: List[Dict[str, Any]],
    allow_pyramiding: bool = False,
) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    """후보 목록에 buyable guard를 일괄 적용한다.

    Returns
    -------
    (buyable_candidates, summary_counts)
    summary_counts: {
        "checked": int,
        "blocked_position": int,
        "blocked_today_buy": int,
        "blocked_today_exit": int,
        "blocked_open_order": int,
        "blocked_pending_confirm": int,
        "buyable": int,
    }
    """
    counts: Dict[str, int] = {
        "checked": 0,
        "blocked_position": 0,
        "blocked_today_buy": 0,
        "blocked_today_exit": 0,
        "blocked_open_order": 0,
        "blocked_pending_confirm": 0,
        "buyable": 0,
    }
    buyable_candidates: List[Dict[str, Any]] = []

    for candidate in candidates:
        code = str(candidate.get("code") or "").zfill(6)
        counts["checked"] += 1

        result = is_symbol_buyable_today(
            code=code,
            session_kind=session_kind,
            trade_date_kst=trade_date_kst,
            current_positions=current_positions,
            open_orders=open_orders,
            today_orders=today_orders,
            today_fills=today_fills,
            today_exit_events=today_exit_events,
            allow_pyramiding=allow_pyramiding,
        )

        if result.buyable:
            counts["buyable"] += 1
            buyable_candidates.append(candidate)
        else:
            reason = result.blocked_reason or ""
            if reason == "BUYABLE_BLOCK_POSITION_EXISTS":
                counts["blocked_position"] += 1
            elif reason in ("BUYABLE_BLOCK_TODAY_EXIT_EXISTS", "BUYABLE_BLOCK_EXIT_COOLDOWN"):
                counts["blocked_today_exit"] += 1
            elif reason == "BUYABLE_BLOCK_OPEN_ORDER_EXISTS":
                counts["blocked_open_order"] += 1
            elif reason == "BUYABLE_BLOCK_TODAY_BUY_EXISTS":
                counts["blocked_today_buy"] += 1
            elif reason == "BUYABLE_BLOCK_PENDING_FILL_CONFIRM":
                counts["blocked_pending_confirm"] += 1

    logger.info(
        "[BUYABLE][GUARD][SUMMARY] session=%s checked=%s "
        "blocked_position=%s blocked_today_buy=%s blocked_today_exit=%s "
        "blocked_open_order=%s blocked_pending_confirm=%s buyable=%s",
        session_kind,
        counts["checked"],
        counts["blocked_position"],
        counts["blocked_today_buy"],
        counts["blocked_today_exit"],
        counts["blocked_open_order"],
        counts["blocked_pending_confirm"],
        counts["buyable"],
    )
    return buyable_candidates, counts


# ---------------------------------------------------------------------------
# Internal helpers — 특정 종목코드 hardcoding 없이 동일 로직
# ---------------------------------------------------------------------------


def _check_today_exit_block(
    *,
    code: str,
    today_orders: List[Dict[str, Any]],
    today_fills: List[Dict[str, Any]],
    today_exit_events: List[Dict[str, Any]],
) -> Optional[str]:
    """당일 해당 code에 STOP 계열 매도가 있으면 exit_reason을 반환한다."""
    # today_exit_events 확인
    for event in today_exit_events or []:
        if str(event.get("code") or "").zfill(6) != code:
            continue
        reason = _normalize_reason(event.get("exit_reason") or event.get("reason"))
        if reason in STOP_EXIT_REASONS:
            return reason

    # today_orders 중 SELL + STOP 계열
    for order in today_orders or []:
        if str(order.get("code") or "").zfill(6) != code:
            continue
        side = _normalize_reason(order.get("side"))
        if side != "SELL":
            continue
        status = _normalize_reason(order.get("status"))
        if status not in ACTIVE_SELL_STATUSES:
            continue
        reason = _normalize_reason(
            order.get("exit_reason") or order.get("reason") or order.get("primary_reason")
        )
        if reason in STOP_EXIT_REASONS:
            return reason

    # today_fills 중 SELL + STOP 계열
    for fill in today_fills or []:
        if str(fill.get("code") or "").zfill(6) != code:
            continue
        side = _normalize_reason(fill.get("side"))
        if side != "SELL":
            continue
        reason = _normalize_reason(
            fill.get("exit_reason") or fill.get("reason") or fill.get("primary_reason")
        )
        if reason in STOP_EXIT_REASONS:
            return reason

    return None


def _get_position_qty(
    *,
    code: str,
    current_positions: List[Dict[str, Any]],
) -> int:
    for pos in current_positions or []:
        if str(pos.get("code") or "").zfill(6) == code:
            qty = int(float(pos.get("qty") or pos.get("hldg_qty") or 0))
            if qty > 0:
                return qty
    return 0


def _find_open_order(
    *,
    code: str,
    open_orders: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """미체결/접수 주문 (BUY 또는 SELL) 검색."""
    for order in open_orders or []:
        if str(order.get("code") or "").zfill(6) == code:
            status = _normalize_reason(order.get("status"))
            if status in {"SUBMITTED", "ACCEPTED", "PENDING_CONFIRM", "PARTIALLY_FILLED"}:
                return order
    return None


def _find_today_buy_order(
    *,
    code: str,
    today_orders: List[Dict[str, Any]],
    today_fills: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """당일 BUY 주문 (어떤 상태든) 검색."""
    for order in today_orders or []:
        if str(order.get("code") or "").zfill(6) != code:
            continue
        side = _normalize_reason(order.get("side"))
        if side != "BUY":
            continue
        status = _normalize_reason(order.get("status"))
        if status in ACTIVE_BUY_STATUSES:
            return order
    for fill in today_fills or []:
        if str(fill.get("code") or "").zfill(6) != code:
            continue
        side = _normalize_reason(fill.get("side"))
        if side == "BUY":
            return fill
    return None


def _find_pending_fill_confirm(
    *,
    code: str,
    today_orders: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """ACCEPTED/PENDING_CONFIRM 상태의 BUY 주문 검색."""
    for order in today_orders or []:
        if str(order.get("code") or "").zfill(6) != code:
            continue
        side = _normalize_reason(order.get("side"))
        if side != "BUY":
            continue
        status = _normalize_reason(order.get("status"))
        if status in {"ACCEPTED", "PENDING_CONFIRM"}:
            return order
    return None


def evaluate_order_submit_result(
    *,
    session_kind: str,
    order_candidates_count: int,
    api_submitted: int,
    skipped: int,
    failed: int,
    rejected: int,
    skip_reasons_summary: Optional[Dict[str, int]] = None,
) -> Optional[Dict[str, Any]]:
    """후보가 있는데 api_submitted=0인 경우 처리 결과를 반환.

    Returns
    -------
    None  — 비정상 상황 (RuntimeError 필요)
    dict  — OK_NO_TRADE 또는 OK_SKIPPED_BY_GUARD (정상 skip)

    Caller는 반환값이 None이면 RuntimeError를 발생시켜야 한다.
    """
    if order_candidates_count <= 0:
        return None  # 후보 없음 → 해당 없음

    if api_submitted > 0:
        return None  # 정상 submit → 해당 없음

    # 후보가 있고 api_submitted=0인 경우
    if skipped == order_candidates_count and failed == 0 and rejected == 0:
        # 모두 guard/cooldown/dedupe로 skip된 정상 케이스
        summary = skip_reasons_summary or {}
        logger.warning(
            "[ORDER][ALL_SKIPPED_BEFORE_SUBMIT] session=%s candidates=%s skipped=%s reasons=%s",
            session_kind,
            order_candidates_count,
            skipped,
            summary,
        )
        logger.info(
            "[RUN_SUMMARY][RESULT] session=%s status=OK_NO_TRADE reason=ALL_CANDIDATES_SKIPPED_BEFORE_API_SUBMIT",
            session_kind,
        )
        return {
            "status": "OK_NO_TRADE",
            "reason": "ALL_CANDIDATES_SKIPPED_BEFORE_API_SUBMIT",
            "details": {
                "session": session_kind,
                "candidates": order_candidates_count,
                "skipped": skipped,
                "skip_reasons": summary,
            },
        }

    # 비정상: 후보 있고, submit=0인데 skipped < candidates이거나 failed/rejected 있음
    logger.error(
        "[ORDER][ANOMALY][CANDIDATE_WITHOUT_API_SUBMIT] session=%s "
        "candidates=%s attempted=0 api_submitted=%s skipped=%s failed=%s rejected=%s",
        session_kind,
        order_candidates_count,
        api_submitted,
        skipped,
        failed,
        rejected,
    )
    return None  # → RuntimeError 발생 신호
