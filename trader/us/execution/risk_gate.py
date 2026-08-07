# -*- coding: utf-8 -*-
"""US Risk Gate.

모든 주문 intent는 이 gate를 통과해야 한다.
환경변수/포지션 한도/현금 버퍼/중복 주문 등을 검증한다.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from trader.us import config as us_cfg
from trader.us.symbols import is_known_symbol, resolve_exchange
from trader.us.execution.kis_us_registry import get_order_exchange_code_for_api
from trader.us.execution.order_permissions import resolve_us_order_permissions

logger = logging.getLogger(__name__)


class RiskGateBlocked(Exception):
    """위험 게이트 통과 실패."""


def _block(reason: str, symbol: str = "", **kw: Any) -> None:
    parts = ["[US_RISK][BLOCK]"]
    if symbol:
        parts.append(f"symbol={symbol}")
    parts.append(f"reason={reason}")
    for k, v in kw.items():
        parts.append(f"{k}={v}")
    msg = " ".join(parts)
    logger.warning(msg)
    raise RiskGateBlocked(msg)


def _pass(symbol: str, notional_usd: float) -> None:
    logger.info("[US_RISK][ALLOW] symbol=%s reason=ALL_GATES_PASSED notional_usd=%.2f", symbol, notional_usd)


def check_env_flags(symbol: str = "", *, session_ok: bool = True, prep_ok: bool = True, balance_ok: bool = True) -> None:
    """Unified US order permission guard (AM/afternoon share this contract)."""
    strategy_env = os.getenv("STRATEGY_ENV", "practice").lower()
    run_mode = os.getenv("RUN_MODE") or os.getenv("STRATEGY_MODE") or "TRADE"
    session = os.getenv("PB1_SESSION") or os.getenv("WSL_RUN_SESSION") or "unknown"
    permission = resolve_us_order_permissions(session, strategy_env, run_mode, os.environ)
    logger.info(
        "[US_ORDER_PERMISSION] session=%s allowed=%d dry_run=%d live=%d us_live=%d armed=%d run_mode=%s",
        session, int(permission.allowed), int(permission.dry_run), int(permission.live_trading_enabled),
        int(permission.us_live_trading_enabled), int(permission.us_order_armed), run_mode,
    )
    if not os.getenv("US_AGENT_ENABLED", "1") in {"1", "true", "TRUE"}:
        _block("us_agent_not_enabled", symbol=symbol, required="US_AGENT_ENABLED=1", current=os.getenv("US_AGENT_ENABLED", "<unset>"))
    if os.getenv("TRADING_REGION", "US").upper() != "US":
        _block("trading_region_not_us", symbol=symbol, required="TRADING_REGION=US", current=os.getenv("TRADING_REGION", "<unset>"))
    if os.getenv("KIS_ENV", "practice").lower() != "practice":
        _block("kis_env_not_practice", symbol=symbol, required="KIS_ENV=practice", current=os.getenv("KIS_ENV", "<unset>"))
    if strategy_env != "practice":
        _block("strategy_env_not_practice", symbol=symbol, required="STRATEGY_ENV=practice", current=os.getenv("STRATEGY_ENV", "<unset>"))
    legacy_paper_unit_mode = (
        os.getenv("US_PAPER_TRADING_ENABLED") == "1"
        and os.getenv("US_ORDER_ARMED") is None
        and os.getenv("LIVE_TRADING_ENABLED", "0") != "1"
    )
    if legacy_paper_unit_mode:
        return
    if not session_ok:
        _block("outside_session_window", symbol=symbol, required="session_window_valid=1", current="0")
    if not prep_ok:
        _block("prep_contract_not_ok", symbol=symbol, required="prep_contract_ok=1", current="0")
    if not balance_ok:
        _block("balance_unavailable", symbol=symbol, required="balance_available=1", current="0")
    if not permission.allowed:
        _block(permission.reasons[0], symbol=symbol)


def check_symbol(symbol: str) -> None:
    """symbol이 universe에 존재하는지 확인."""
    if not is_known_symbol(symbol):
        _block("symbol_not_in_universe", symbol=symbol)


def check_exchange(exchange: str) -> None:
    """exchange가 registry에 존재하는지 확인."""
    try:
        get_order_exchange_code_for_api(exchange)
    except ValueError:
        _block("exchange_not_in_registry", exchange=exchange)


def check_qty(qty: int) -> None:
    if qty <= 0:
        _block("invalid_qty", qty=qty)


def check_notional(notional_usd: float, symbol: str = "") -> None:
    limit = float(os.getenv("US_MAX_ORDER_USD", "100"))
    if notional_usd > limit:
        _block(
            "notional_exceeds_order_limit",
            symbol=symbol,
            notional_usd=notional_usd,
            limit=limit,
        )


def check_daily_notional(
    new_notional_usd: float,
    current_daily_notional_usd: float,
    symbol: str = "",
    *,
    current_filled_notional: float | None = None,
    current_acknowledged_notional: float | None = None,
    current_pending_notional: float | None = None,
    current_reserved_notional: float | None = None,
    current_risk_total_notional: float | None = None,
) -> None:
    filled = float(current_filled_notional if current_filled_notional is not None else current_daily_notional_usd)
    acknowledged = float(current_acknowledged_notional if current_acknowledged_notional is not None else filled)
    pending = float(current_pending_notional if current_pending_notional is not None else 0.0)
    reserved = float(current_reserved_notional if current_reserved_notional is not None else max(0.0, acknowledged - filled))
    risk_total = float(
        current_risk_total_notional
        if current_risk_total_notional is not None
        else max(current_daily_notional_usd, filled + pending + reserved)
    )
    total = risk_total + new_notional_usd
    limit = float(os.getenv("US_MAX_DAILY_NOTIONAL_USD", "500"))
    if total > limit:
        _block(
            "daily_notional_exceeded",
            symbol=symbol,
            new_notional=new_notional_usd,
            current_total=risk_total,
            current_filled_notional=round(filled, 4),
            current_acknowledged_notional=round(acknowledged, 4),
            current_pending_notional=round(pending, 4),
            current_reserved_notional=round(reserved, 4),
            current_risk_total_notional=round(risk_total, 4),
            limit=limit,
        )


def check_position_count(
    current_count: int,
    symbol: str = "",
    *,
    reason: str = "max_positions_reached",
) -> None:
    limit = int(os.getenv("US_MAX_POSITIONS", "10"))
    if current_count >= limit:
        _block(reason, symbol=symbol, count=current_count, limit=limit)


def check_position_weight(
    notional_usd: float,
    total_portfolio_usd: float,
    symbol: str = "",
    current_position_market_value_usd: float = 0.0,
) -> None:
    if total_portfolio_usd <= 0:
        return
    # Router/unit-test callers may omit account equity and leave the historical
    # 1000 USD default. Use configured US_ACCOUNT_EQUITY_USD as the reference
    # when available so the 5% limit is applied to account equity, not a tiny
    # placeholder.
    try:
        env_equity = float(os.getenv("US_ACCOUNT_EQUITY_USD", "0") or 0)
    except (TypeError, ValueError):
        env_equity = 0.0
    try:
        cap_krw = float(os.getenv("US_PAPER_MAX_CAPITAL_KRW", "0") or 0)
        fx = float(os.getenv("US_BUDGET_FX_KRW_PER_USD", "1450") or 1450)
        env_equity = max(env_equity, cap_krw / fx if fx > 0 else 0.0)
    except (TypeError, ValueError):
        pass
    reference_portfolio_usd = max(float(total_portfolio_usd), env_equity)
    projected_position_value = max(0.0, current_position_market_value_usd) + notional_usd
    weight = projected_position_value / reference_portfolio_usd
    limit = float(os.getenv("US_MAX_POSITION_WEIGHT", "0.05"))
    if weight > limit:
        _block(
            "position_weight_exceeded",
            symbol=symbol,
            weight=round(weight, 4),
            projected_position_value=round(projected_position_value, 4),
            limit=limit,
        )


def check_cash_buffer(
    available_cash_usd: float,
    order_notional_usd: float,
    symbol: str = "",
) -> None:
    buffer = float(os.getenv("US_MIN_CASH_BUFFER_USD", "50"))
    remaining = available_cash_usd - order_notional_usd
    if remaining < buffer:
        _block(
            "cash_below_buffer",
            symbol=symbol,
            remaining=remaining,
            buffer=buffer,
        )


def check_duplicate(client_order_key: str, existing_keys: set[str]) -> None:
    if client_order_key in existing_keys:
        msg = f"[US_DUPLICATE][BLOCK] client_order_key={client_order_key!r} already exists"
        logger.warning(msg)
        raise RiskGateBlocked(msg)


def check_us_capital_budget(
    order_notional_usd: float,
    available_cash_usd: float,
    symbol: str = "",
) -> None:
    """미국장 5천만원 환산 예산 한도 초과 여부 검증.

    [US_RISK][BUDGET] 로그 출력 후
    effective_order_budget_usd 를 초과하면 us_capital_budget_exceeded 차단.
    """
    from trader.us.budget import resolve_us_order_budget

    budget = resolve_us_order_budget(available_cash_usd)
    effective = budget["effective_order_budget_usd"]

    logger.info(
        "[US_RISK][BUDGET] symbol=%s effective_budget=%.2f notional=%.2f cap_usd=%.2f",
        symbol, effective, order_notional_usd, budget["capital_usd_cap"],
    )

    if order_notional_usd > effective:
        _block(
            "us_capital_budget_exceeded",
            symbol=symbol,
            notional_usd=order_notional_usd,
            effective_budget=effective,
        )


def check_same_day_rebuy(symbol: str, side: str) -> None:
    """당일 매도 후 재매수 차단.

    US_BLOCK_REBUY_AFTER_SELL_SAME_DAY=1 일 때만 활성화.
    """
    from trader.utils.env import env_bool
    if not env_bool("US_BLOCK_REBUY_AFTER_SELL_SAME_DAY", default=False):
        return

    if side.upper() != "BUY":
        return

    try:
        from trader.us.db.repos import load_today_symbols_sold
        sold_today = load_today_symbols_sold()
        if symbol in sold_today:
            _block("same_day_rebuy_block", symbol=symbol)
    except RiskGateBlocked:
        raise
    except Exception as exc:
        logger.warning("[US_RISK][WARN] same_day_rebuy check failed: %s", exc)


def check_pending_order(symbol: str, side: str, trade_date: str | None = None) -> None:
    """미체결 주문 존재 시 추가 주문 차단.

    US_ORDER_ACCEPTED_IS_NOT_FILLED=1 일 때만 활성화.
    """
    from trader.utils.env import env_bool
    if not env_bool("US_ORDER_ACCEPTED_IS_NOT_FILLED", default=False):
        return

    try:
        from trader.us.db.repos import has_pending_order_for_symbol_side
        if has_pending_order_for_symbol_side(symbol=symbol, side=side, trade_date=trade_date):
            _block("pending_order_exists", symbol=symbol, side=side)
    except RiskGateBlocked:
        raise
    except Exception as exc:
        logger.warning("[US_RISK][WARN] pending_order check failed: %s", exc)


def check_pending_sell_order_hard(symbol: str, trade_date: str | None = None) -> None:
    """SELL idempotency hard gate independent of US_ORDER_ACCEPTED_IS_NOT_FILLED."""
    try:
        from trader.us.db.repos import has_pending_order_for_symbol_side
        if has_pending_order_for_symbol_side(
            symbol=symbol,
            side="SELL",
            trade_date=trade_date,
            include_statuses={
                "SUBMITTED", "ACK", "PENDING", "PARTIALLY_FILLED",
                "RECONCILE_PENDING", "ACK_DB_FAILED",
            },
        ):
            _block("pending_sell_order_exists", symbol=symbol, side="SELL")
    except RiskGateBlocked:
        raise
    except Exception as exc:
        logger.warning("[US_RISK][WARN] pending_sell_order_hard check failed: %s", exc)


def check_entry_cutoff(side: str, now: Any = None) -> None:
    """15:45 ET 이후 신규 매수 차단.

    US_BLOCK_NEW_ENTRY_AFTER_ET 환경변수가 명시적으로 설정된 경우에만 활성화한다.
    """
    if side.upper() != "BUY":
        return

    cutoff_str = os.getenv("US_BLOCK_NEW_ENTRY_AFTER_ET", "")
    if not cutoff_str:
        # 명시적으로 설정되지 않으면 검사하지 않음
        return
    try:
        from zoneinfo import ZoneInfo
        from datetime import time as dtime
        NY_TZ = ZoneInfo("America/New_York")

        if now is None:
            from trader.us.market_calendar import now_ny
            now_dt = now_ny()
        else:
            if hasattr(now, "astimezone"):
                now_dt = now.astimezone(NY_TZ)
            else:
                now_dt = now

        h, m = cutoff_str.split(":")
        cutoff = dtime(int(h), int(m))

        if now_dt.time() >= cutoff:
            _block(
                "after_entry_cutoff",
                cutoff=cutoff_str,
                current_et=now_dt.strftime("%H:%M:%S"),
            )
    except RiskGateBlocked:
        raise
    except Exception as exc:
        logger.warning("[US_RISK][WARN] entry cutoff check failed: %s", exc)


def check_symbol_contract(
    symbol: str,
    side: str,
    *,
    allowed_symbols: "set[str] | None" = None,
    current_position_symbols: "set[str] | None" = None,
) -> None:
    """BUY/SELL 방향별 universe 검증.

    BUY: locked watchlist (allowed_symbols) 기준. 없으면 정적 레지스트리 fallback.
    SELL: 보유 포지션 (current_position_symbols) 기준. 없으면 정적 레지스트리 fallback.
    심볼을 하드코딩하지 않는다. 모든 판단은 전달받은 집합을 기준으로 한다.
    """
    sym = str(symbol).strip().upper()
    side_upper = side.upper()

    if side_upper == "BUY":
        if allowed_symbols is not None:
            count = len(allowed_symbols)
            logger.info(
                "[US_RISK][UNIVERSE] source=locked_watchlist side=BUY count=%d",
                count,
            )
            if sym not in allowed_symbols:
                _block("symbol_not_in_universe", symbol=sym)
        else:
            # fallback: 정적 레지스트리
            check_symbol(sym)
    else:  # SELL
        if current_position_symbols is not None:
            if sym in current_position_symbols:
                logger.info(
                    "[US_RISK][SELL_UNIVERSE_PASS] symbol=%s source=current_positions",
                    sym,
                )
                return
            # 보유 포지션에도 없지만 locked watchlist에도 있으면 통과
            if allowed_symbols is not None and sym in allowed_symbols:
                logger.info(
                    "[US_RISK][SELL_UNIVERSE_PASS] symbol=%s source=locked_watchlist",
                    sym,
                )
                return
            # 둘 다 없으면 차단
            _block("symbol_not_in_universe", symbol=sym)
        else:
            # fallback: 정적 레지스트리
            check_symbol(sym)


def assert_order_allowed(
    intent: dict,
    *,
    current_daily_notional_usd: float = 0.0,
    current_position_count: int = 0,
    total_portfolio_usd: float = 1000.0,
    available_cash_usd: float = 1000.0,
    existing_order_keys: "set[str] | None" = None,
    now: Any = None,
    allowed_symbols: "set[str] | None" = None,
    current_position_symbols: "set[str] | None" = None,
    trade_date: str | None = None,
    is_existing_position_buy: bool = False,
    current_filled_notional: float | None = None,
    current_acknowledged_notional: float | None = None,
    current_pending_notional: float | None = None,
    current_reserved_notional: float | None = None,
    current_risk_total_notional: float | None = None,
) -> None:
    """Order intent의 전체 위험 점검.

    모든 체크를 통과하면 [US_RISK][PASS] 로그.
    하나라도 실패하면 RiskGateBlocked 예외.

    allowed_symbols: BUY 허용 심볼 집합 (locked watchlist).
    current_position_symbols: 현재 보유 심볼 집합 (SELL universe).
    """
    symbol = intent.get("symbol", "")
    exchange = intent.get("exchange", "")
    side = intent.get("side", "BUY")
    qty = int(intent.get("qty", 0))
    notional_usd = float(intent.get("notional_usd", 0.0))
    client_order_key = intent.get("client_order_key", "")

    check_env_flags(symbol=symbol)
    check_symbol_contract(
        symbol,
        side,
        allowed_symbols=allowed_symbols,
        current_position_symbols=current_position_symbols,
    )
    check_exchange(exchange)
    check_qty(qty)

    if existing_order_keys is not None and client_order_key:
        check_duplicate(client_order_key, existing_order_keys)

    # SELL: 보유 수량 초과 차단
    if side.upper() == "SELL":
        available_qty = intent.get("available_qty")
        if available_qty is not None:
            try:
                if qty > int(available_qty):
                    _block("sell_qty_exceeds_position",
                           symbol=symbol, qty=qty, available_qty=available_qty)
            except (TypeError, ValueError):
                pass
        # SELL 전용: 미체결 주문 존재 시 매도 차단
        check_pending_sell_order_hard(symbol, trade_date=trade_date)
    else:
        # BUY 전용 체크
        # 예산 기반 차단 (US_PAPER_MAX_CAPITAL_KRW 기준 5천만원 환산)
        check_us_capital_budget(notional_usd, available_cash_usd, symbol=symbol)

        check_notional(notional_usd, symbol=symbol)
        check_daily_notional(
            notional_usd,
            current_daily_notional_usd,
            symbol=symbol,
            current_filled_notional=current_filled_notional,
            current_acknowledged_notional=current_acknowledged_notional,
            current_pending_notional=current_pending_notional,
            current_reserved_notional=current_reserved_notional,
            current_risk_total_notional=current_risk_total_notional,
        )
        if is_existing_position_buy:
            logger.info(
                "[US_RISK][POSITION_COUNT_SKIP] symbol=%s reason=existing_position_add_buy count=%s limit=%s",
                symbol,
                current_position_count,
                os.getenv("US_MAX_POSITIONS", "30"),
            )
        else:
            check_position_count(
                current_position_count,
                symbol=symbol,
                reason="max_positions_reached_new_symbol",
            )
        capital_meta = (intent.get("meta") or {}).get("capital_deployment") or {}
        current_position_market_value_usd = 0.0
        for _value in (
            intent.get("current_position_market_value_usd"),
            capital_meta.get("current_position_market_value_usd"),
            intent.get("current_market_value_usd"),
            intent.get("market_value_usd"),
        ):
            try:
                if _value is not None:
                    current_position_market_value_usd = float(_value or 0.0)
                    break
            except (TypeError, ValueError):
                pass
        projected_weight = intent.get("projected_weight")
        if projected_weight is None:
            projected_weight = capital_meta.get("projected_weight")
        if projected_weight is not None:
            try:
                projected_weight_f = float(projected_weight)
                limit = float(os.getenv("US_MAX_POSITION_WEIGHT", "0.05"))
                if projected_weight_f > limit:
                    _block(
                        "position_weight_exceeded",
                        symbol=symbol,
                        weight=round(projected_weight_f, 4),
                        projected_weight=round(projected_weight_f, 4),
                        limit=limit,
                    )
            except (TypeError, ValueError):
                pass
        check_position_weight(notional_usd, total_portfolio_usd, symbol=symbol, current_position_market_value_usd=current_position_market_value_usd)
        check_cash_buffer(available_cash_usd, notional_usd, symbol=symbol)

        check_same_day_rebuy(symbol, side)
        check_pending_order(symbol, side, trade_date=trade_date)
        check_entry_cutoff(side, now=now)

    _pass(symbol, notional_usd)
