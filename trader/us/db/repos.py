# -*- coding: utf-8 -*-
"""US DB Repository Functions.

us_* 테이블에 대한 CRUD 함수 모음.

DB 접근 방식:
- PBCORE_DB_URL이 없으면 (offline/test) 메모리 저장소 사용
- 실제 DB는 SQLAlchemy (text()) 또는 psycopg2로 직접 접근

중요:
- KR 테이블(orders, positions, signals, pb1_*)에 절대 접근하지 않는다.
- 모든 테이블명은 us_ 접두사를 사용한다.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory fallback store (offline / test 환경)
# ---------------------------------------------------------------------------

_MEM_WATCHLIST: list[dict] = []
_MEM_INTENTS: list[dict] = []
_MEM_ORDERS: list[dict] = []
_MEM_FILLS: list[dict] = []
_MEM_POSITIONS: list[dict] = []
_MEM_RECONCILE_LOGS: list[dict] = []


def _db_available() -> bool:
    """DB URL이 설정되어 있으면 True."""
    return bool(os.getenv("PBCORE_DB_URL"))


def _get_conn():
    """DB 커넥션 반환 (없으면 None)."""
    url = os.getenv("PBCORE_DB_URL")
    if not url:
        return None
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(url)
    except Exception as exc:
        logger.warning("[US_DB][WARN] DB connection failed: %s", exc)
        return None


def _today() -> str:
    return date.today().isoformat()


# ---------------------------------------------------------------------------
# Watchlist
# ---------------------------------------------------------------------------

def save_us_watchlist(entries: list[dict], trade_date: str | None = None) -> int:
    """us_watchlist에 관심 종목 저장.

    Args:
        entries: [{"symbol", "exchange", "strategy", "score", "meta"}, ...]
        trade_date: 거래일 (None이면 오늘)

    Returns:
        저장된 row 수
    """
    td = trade_date or _today()
    count = 0

    conn = _get_conn()
    if conn is None:
        for e in entries:
            e["trade_date"] = td
            _MEM_WATCHLIST.append(e)
        logger.info("[US_WATCHLIST][SAVE] count=%d (in-memory)", len(entries))
        return len(entries)

    try:
        with conn:
            with conn.cursor() as cur:
                for e in entries:
                    cur.execute(
                        """
                        INSERT INTO us_watchlist
                            (trade_date, symbol, exchange, strategy, score, meta)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (trade_date, symbol, strategy) DO UPDATE
                            SET score = EXCLUDED.score,
                                meta  = EXCLUDED.meta
                        """,
                        (
                            td,
                            e.get("symbol"),
                            e.get("exchange", "NASDAQ"),
                            e.get("strategy", "us_pb1"),
                            e.get("score"),
                            str(e.get("meta", {})),
                        ),
                    )
                    count += 1
    except Exception as exc:
        logger.error("[US_WATCHLIST][ERROR] %s", exc)
    finally:
        conn.close()

    logger.info("[US_WATCHLIST][SAVE] count=%d (db)", count)
    return count


# ---------------------------------------------------------------------------
# Order Intents
# ---------------------------------------------------------------------------

def save_order_intent(intent: dict, trade_date: str | None = None) -> bool:
    """us_order_intents에 단일 intent 저장."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        _MEM_INTENTS.append({**intent, "trade_date": td, "status": "PENDING"})
        return True

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO us_order_intents
                        (trade_date, client_order_key, symbol, exchange, side, qty,
                         limit_price_usd, notional_usd, strategy, status, meta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (client_order_key) DO NOTHING
                    """,
                    (
                        td,
                        intent.get("client_order_key"),
                        intent.get("symbol"),
                        intent.get("exchange", "NASDAQ"),
                        intent.get("side", "BUY"),
                        int(intent.get("qty", 0)),
                        intent.get("limit_price"),
                        intent.get("notional_usd"),
                        intent.get("strategy", "us_pb1"),
                        "PENDING",
                        str(intent.get("meta", {})),
                    ),
                )
        return True
    except Exception as exc:
        logger.error("[US_INTENT][SAVE][ERROR] %s", exc)
        return False
    finally:
        conn.close()


def load_open_order_intents(trade_date: str | None = None) -> list[dict]:
    """당일 PENDING 상태 order intent 목록 반환."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        return [i for i in _MEM_INTENTS if i.get("trade_date") == td and i.get("status") == "PENDING"]

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM us_order_intents WHERE trade_date = %s AND status = 'PENDING'",
                    (td,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("[US_INTENT][LOAD][ERROR] %s", exc)
        return []
    finally:
        conn.close()


def mark_order_intent_sent(client_order_key: str) -> None:
    conn = _get_conn()
    if conn is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "SENT"
        return

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE us_order_intents SET status = 'SENT' WHERE client_order_key = %s",
                    (client_order_key,),
                )
    except Exception as exc:
        logger.error("[US_INTENT][MARK_SENT][ERROR] %s", exc)
    finally:
        conn.close()


def mark_order_intent_blocked(client_order_key: str, reason: str = "") -> None:
    conn = _get_conn()
    if conn is None:
        for i in _MEM_INTENTS:
            if i.get("client_order_key") == client_order_key:
                i["status"] = "BLOCKED"
                i["block_reason"] = reason
        return

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE us_order_intents SET status = 'BLOCKED' WHERE client_order_key = %s",
                    (client_order_key,),
                )
    except Exception as exc:
        logger.error("[US_INTENT][MARK_BLOCKED][ERROR] %s", exc)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orders (ACK)
# ---------------------------------------------------------------------------

def save_order_ack(order_result: dict, trade_date: str | None = None) -> bool:
    """us_orders에 주문 ACK 저장."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        _MEM_ORDERS.append({**order_result, "trade_date": td})
        return True

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO us_orders
                        (trade_date, client_order_key, symbol, exchange, side, qty,
                         order_type, limit_price_usd, notional_usd, status, raw_response)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (client_order_key) DO NOTHING
                    """,
                    (
                        td,
                        order_result.get("client_order_key", ""),
                        order_result.get("symbol", ""),
                        order_result.get("exchange", "NASDAQ"),
                        order_result.get("side", "BUY"),
                        int(order_result.get("qty", 0)),
                        "LIMIT",
                        order_result.get("limit_price"),
                        order_result.get("notional_usd"),
                        order_result.get("status", "ACK"),
                        str(order_result.get("raw", {})),
                    ),
                )
        return True
    except Exception as exc:
        logger.error("[US_ORDER_ACK][SAVE][ERROR] %s", exc)
        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fills
# ---------------------------------------------------------------------------

def save_fills(fills: list[dict], trade_date: str | None = None) -> int:
    """us_fills에 체결 내역 저장."""
    td = trade_date or _today()
    count = 0

    conn = _get_conn()
    if conn is None:
        for f in fills:
            _MEM_FILLS.append({**f, "trade_date": td})
        return len(fills)

    try:
        with conn:
            with conn.cursor() as cur:
                for f in fills:
                    cur.execute(
                        """
                        INSERT INTO us_fills
                            (trade_date, symbol, exchange, side, qty, fill_price_usd,
                             order_no, filled_at_str)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            td,
                            f.get("symbol"),
                            f.get("exchange", ""),
                            f.get("side"),
                            int(f.get("qty", 0)),
                            float(f.get("price", 0)),
                            f.get("order_no", ""),
                            f.get("filled_at", ""),
                        ),
                    )
                    count += 1
    except Exception as exc:
        logger.error("[US_FILLS][SAVE][ERROR] %s", exc)
    finally:
        conn.close()

    return count


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

def save_position_snapshot(positions: list[dict], trade_date: str | None = None) -> int:
    """us_positions에 포지션 스냅샷 저장 (upsert)."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        _MEM_POSITIONS.clear()
        for p in positions:
            _MEM_POSITIONS.append({**p, "trade_date": td})
        return len(positions)

    count = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for p in positions:
                    cur.execute(
                        """
                        INSERT INTO us_positions
                            (trade_date, symbol, exchange, qty, avg_price_usd,
                             current_price_usd, unrealized_pnl_usd, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (trade_date, symbol) DO UPDATE
                            SET qty               = EXCLUDED.qty,
                                avg_price_usd     = EXCLUDED.avg_price_usd,
                                current_price_usd = EXCLUDED.current_price_usd,
                                unrealized_pnl_usd= EXCLUDED.unrealized_pnl_usd,
                                status            = EXCLUDED.status
                        """,
                        (
                            td,
                            p.get("symbol"),
                            p.get("exchange", "NASDAQ"),
                            int(p.get("qty", 0)),
                            float(p.get("entry_price", 0)),
                            float(p.get("current_price", 0)),
                            float(p.get("unrealized_pnl_usd", 0)),
                            p.get("status", "OPEN"),
                        ),
                    )
                    count += 1
    except Exception as exc:
        logger.error("[US_POSITIONS][SNAPSHOT][ERROR] %s", exc)
    finally:
        conn.close()

    logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", count)
    return count


def load_positions(trade_date: str | None = None) -> list[dict]:
    """당일 open 포지션 목록 반환."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        return [p for p in _MEM_POSITIONS if p.get("trade_date") == td]

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM us_positions WHERE trade_date = %s AND status = 'OPEN'",
                    (td,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("[US_POSITIONS][LOAD][ERROR] %s", exc)
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def load_today_symbols_sold(trade_date: str | None = None) -> set[str]:
    """당일 SELL 체결된 종목 집합 반환."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        return {f["symbol"] for f in _MEM_FILLS
                if f.get("trade_date") == td and f.get("side") == "SELL"}

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT symbol FROM us_fills WHERE trade_date = %s AND side = 'SELL'",
                    (td,),
                )
                return {row[0] for row in cur.fetchall()}
    except Exception as exc:
        logger.error("[US_FILLS][SOLD_TODAY][ERROR] %s", exc)
        return set()
    finally:
        conn.close()


def load_today_order_keys(trade_date: str | None = None) -> set[str]:
    """당일 발송된 주문 key 집합 반환 (중복 주문 차단용)."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        return {o["client_order_key"] for o in _MEM_ORDERS
                if o.get("trade_date") == td and o.get("client_order_key")}

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT client_order_key FROM us_orders WHERE trade_date = %s",
                    (td,),
                )
                return {row[0] for row in cur.fetchall()}
    except Exception as exc:
        logger.error("[US_ORDERS][KEYS][ERROR] %s", exc)
        return set()
    finally:
        conn.close()


def load_open_orders_by_symbol(symbol: str, trade_date: str | None = None) -> list[dict]:
    """특정 symbol의 당일 미체결(ACK) 주문 목록 반환."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        return [o for o in _MEM_ORDERS
                if o.get("symbol") == symbol
                and o.get("trade_date") == td
                and o.get("status") in ("ACK", "SENT")]

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM us_orders
                    WHERE symbol = %s AND trade_date = %s
                      AND status IN ('ACK', 'SENT', 'PENDING')
                    """,
                    (symbol, td),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("[US_ORDERS][OPEN_SYMBOL][ERROR] %s", exc)
        return []
    finally:
        conn.close()


def has_pending_order(symbol: str, trade_date: str | None = None) -> bool:
    """해당 symbol의 당일 미체결 주문이 있으면 True."""
    return len(load_open_orders_by_symbol(symbol, trade_date)) > 0


def has_position(symbol: str, trade_date: str | None = None) -> bool:
    """해당 symbol의 당일 보유 포지션이 있으면 True."""
    positions = load_positions(trade_date)
    return any(p.get("symbol") == symbol for p in positions)


# ---------------------------------------------------------------------------
# Reconcile log
# ---------------------------------------------------------------------------

def save_reconcile_log(log: dict, trade_date: str | None = None) -> bool:
    """us_reconcile_logs에 reconcile 결과 저장."""
    td = trade_date or _today()

    conn = _get_conn()
    if conn is None:
        _MEM_RECONCILE_LOGS.append({**log, "trade_date": td})
        return True

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO us_reconcile_logs
                        (trade_date, status, position_count, total_pvs_usd, detail)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        td,
                        log.get("status", "OK"),
                        int(log.get("position_count", 0)),
                        float(log.get("total_pvs", 0)),
                        str(log.get("detail", {})),
                    ),
                )
        return True
    except Exception as exc:
        logger.error("[US_RECONCILE_LOG][SAVE][ERROR] %s", exc)
        return False
    finally:
        conn.close()
