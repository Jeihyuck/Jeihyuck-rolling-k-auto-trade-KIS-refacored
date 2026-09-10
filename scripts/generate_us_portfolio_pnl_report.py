#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scripts/generate_us_portfolio_pnl_report.py

미국장 Portfolio PNL Report 생성기.

목적:
- GitHub Actions 종료 후 반드시 미국장 PnL 리포트가 생성되도록 보장
- KIS balance / DB snapshot / latest_daily_report.json 순서로 데이터 소스 확보
- 데이터 부족 시에도 FAILED_PNL_REPORT 또는 PARTIAL 상태로 파일 생성
- JSON / MD / CSV 모두 생성

데이터 소스 우선순위:
1. KIS balance (모의투자 계좌 조회)
2. DB us_positions / us_fills
3. latest_us_daily_report.json
4. 없으면 FAILED_PNL_REPORT 상태

필수 생성 파일:
- reports/us_pnl/latest_us_pnl_report.json
- reports/us_pnl/latest_us_pnl_report.md
- reports/us_pnl/latest_us_pnl_report.csv
- reports/us_pnl/{trade_date}/{session}/us_pnl_report.json
- reports/us_pnl/{trade_date}/{session}/us_pnl_report.md
- reports/us_pnl/{trade_date}/{session}/us_pnl_report.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from trader.us.utils.logging_utils import setup_us_logging_once

setup_us_logging_once()
logger = logging.getLogger("generate_us_pnl_report")

# ── 경로 설정 ────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from zoneinfo import ZoneInfo
NY_TZ = ZoneInfo("America/New_York")


def _now_ny() -> datetime:
    return datetime.now(NY_TZ)


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _fmt_usd(v: float) -> str:
    """USD 포맷: +1,234.56"""
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:,.2f}"


def _fmt_pct(v: float) -> str:
    """% 포맷: +12.34%"""
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def _load_engine():
    """DB engine 로드. 실패 시 None 반환."""
    try:
        from trader.db.engine import get_engine

        if not (os.getenv("PBCORE_DB_URL") or os.getenv("DATABASE_URL")):
            logger.warning("[US_PNL][DB_SKIP] PBCORE_DB_URL not set")
            return None

        return get_engine()
    except Exception as exc:
        logger.warning("[US_PNL][DB_ENGINE_FAIL] err=%s", exc)
        traceback.print_exc()
        return None


def _get_table_columns(engine, table_name: str) -> set:
    """DB 테이블 컬럼 목록 조회. 실패 시 빈 set 반환."""
    try:
        from sqlalchemy import text
        with engine.begin() as conn:
            rows = conn.execute(
                text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = :table_name
                """),
                {"table_name": table_name},
            ).fetchall()
        return {r[0] for r in rows}
    except Exception as exc:
        logger.warning("[US_PNL][TABLE_COLUMNS][FAIL] table=%s err=%s", table_name, exc)
        return set()


def _first_existing(d: dict, candidates: list, default=None):
    """후보 키 목록 중 dict에 실제로 존재하는 첫 번째 값 반환."""
    for k in candidates:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _normalize_kis_position(pos: dict) -> dict:
    """KIS 해외잔고 필드를 표준 필드로 정규화.

    trader.us.utils.pnl_utils.normalize_us_position을 재사용한다.
    """
    try:
        from trader.us.utils.pnl_utils import normalize_us_position
    except ImportError:
        normalize_us_position = None

    # symbol 필드 보강
    enriched = dict(pos)
    if "symbol" not in enriched or not enriched["symbol"]:
        enriched["symbol"] = (
            enriched.get("ovrs_pdno")
            or enriched.get("pdno")
            or enriched.get("ticker")
            or enriched.get("code")
            or ""
        )

    if normalize_us_position is not None:
        normalized = normalize_us_position(enriched)
    else:
        # fallback: 직접 파싱
        symbol = _first_existing(enriched, ["symbol", "ovrs_pdno", "pdno", "code", "ticker"], default="")
        qty = _safe_int(_first_existing(enriched, ["qty", "quantity", "ovrs_cblc_qty", "hldg_qty", "cblc_qty", "ord_psbl_qty"], default=0))
        avg_price = _safe_float(_first_existing(enriched, ["avg_price", "average_price", "pchs_avg_pric", "frcr_pchs_avg_pric", "avg_buy_price", "avg_cost", "entry_price"], default=0.0))
        last_price = _safe_float(_first_existing(enriched, ["last_price", "current", "current_price", "current_price_usd", "market_price", "now_price", "now_price2", "now_pric2", "ovrs_now_pric", "ovrs_now_pric1", "ovrs_now_pric2", "ovrs_prpr", "prpr", "stck_prpr", "current_px"], default=0.0))
        normalized = {
            "symbol": str(symbol).strip().upper(),
            "qty": qty,
            "avg_price": avg_price,
            "last_price": last_price,
            "cost_basis_usd": avg_price * qty,
            "market_value_usd": last_price * qty,
        }

    # KIS 전용 추가 필드 (market_value_usd, cost_usd)
    market_value_usd = _safe_float(_first_existing(
        enriched,
        ["market_value_usd", "market_value", "ovrs_stck_evlu_amt", "frcr_evlu_amt2", "ovrs_evlu_amt", "evlu_amt"],
        default=0.0,
    ))
    cost_usd = _safe_float(_first_existing(
        enriched,
        ["cost_usd", "cost_basis_usd", "cost", "purchase_amount", "pchs_amt", "frcr_pchs_amt", "frcr_pchs_amt1", "pchs_amt_smtl_amt", "ovrs_stck_pchs_amt"],
        default=0.0,
    ))

    lp = normalized.get("last_price", 0.0) or 0.0
    qty_n = normalized.get("qty", 0) or 0
    avg_p = normalized.get("avg_price", 0.0) or 0.0

    # last_price를 market_value에서 역산
    if lp <= 0 and market_value_usd > 0 and qty_n > 0:
        lp = market_value_usd / qty_n

    if market_value_usd <= 0:
        market_value_usd = normalized.get("market_value_usd", 0.0) or (lp * qty_n)

    if cost_usd <= 0:
        cost_usd = normalized.get("cost_basis_usd", 0.0) or (avg_p * qty_n)

    return {
        "symbol": normalized.get("symbol", ""),
        "qty": qty_n,
        "avg_price": avg_p,
        "last_price": lp,
        "current": lp,
        "current_price_usd": lp,
        "market_value_usd": market_value_usd,
        "cost_usd": cost_usd,
        "source": "kis_balance",
    }


def _load_kis_balance(env: str) -> dict | None:
    """KIS  practice 잔고 조회. 실패 시 None."""
    try:
        logger.info("[US_PNL][KIS_BALANCE][TRY] env=%s", env)

        from trader.us.execution.kis_us_client import KisUSClient

        kis = KisUSClient(env=env)
        raw = kis.get_us_balance()

        positions = raw.get("output1", [])
        if isinstance(positions, dict):
            positions = [positions]
        elif not isinstance(positions, list):
            positions = []

        queried_exchanges = list(raw.get("queried_exchanges") or [])
        raw_by_exchange = raw.get("raw_by_exchange") or {}
        failed_exchanges = raw.get("failed_exchanges") or {}
        success_exchanges = [exchange for exchange in queried_exchanges if exchange in raw_by_exchange]
        failed_exchange_list = [exchange for exchange in queried_exchanges if exchange in failed_exchanges]
        raw_count = _safe_int(raw.get("raw_count") or sum((raw.get("exchange_result_counts") or {}).values()))
        duplicate_skipped = _safe_int(raw.get("duplicate_skipped") or max(0, raw_count - len(positions)))

        # KIS 필드 정규화
        normalized = [_normalize_kis_position(p) for p in positions]
        # symbol이 비어 있거나 qty=0인 항목 필터링
        normalized = [p for p in normalized if p["symbol"] and p["qty"] > 0]

        if not success_exchanges:
            kis_balance_status = "FAILED_ALL_EXCHANGES"
            logger.warning(
                "[US_BALANCE][FAILED_ALL_EXCHANGES] exchanges=%s reason=%s",
                ",".join(queried_exchanges),
                ",".join(f"{exchange}:{failed_exchanges.get(exchange, 'api_error')}" for exchange in queried_exchanges) or "api_error",
            )
        elif failed_exchange_list:
            kis_balance_status = "PARTIAL"
            logger.warning(
                "[US_BALANCE][PARTIAL] success=%s failed=%s",
                ",".join(success_exchanges),
                ",".join(failed_exchange_list),
            )
        else:
            kis_balance_status = "OK"

        logger.info(
            "[US_BALANCE][MERGED] raw_count=%d unique_symbols=%d duplicate_skipped=%d",
            raw_count,
            len(normalized),
            duplicate_skipped,
        )

        if kis_balance_status == "FAILED_ALL_EXCHANGES":
            logger.warning("[US_PNL][KIS_BALANCE][FAILED] status=FAILED_ALL_EXCHANGES positions_raw=%d positions_valid=%d", len(positions), len(normalized))
        elif kis_balance_status == "PARTIAL":
            logger.warning("[US_PNL][KIS_BALANCE][PARTIAL] positions_raw=%d positions_valid=%d", len(positions), len(normalized))
        else:
            logger.info("[US_PNL][KIS_BALANCE][OK] positions_raw=%d positions_valid=%d", len(positions), len(normalized))

        return {
            "raw": raw,
            "positions": normalized,
            "summary": raw.get("output2", {}),
            "queried_exchanges": queried_exchanges,
            "success_exchanges": success_exchanges,
            "failed_exchanges": failed_exchanges,
            "kis_balance_status": kis_balance_status,
            "raw_count": raw_count,
            "unique_symbols": len(normalized),
            "duplicate_skipped": duplicate_skipped,
        }
    except Exception as exc:
        logger.warning("[US_PNL][KIS_BALANCE][FAIL] err=%s", exc)
        traceback.print_exc()
        return None


def _load_db_positions(engine, trade_date: str) -> list[dict]:
    """DB us_positions에서 당일 포지션 조회. 컬럼 alias 자동 처리."""
    try:
        from sqlalchemy import text

        # 실제 컬럼 목록 조회
        cols = _get_table_columns(engine, "us_positions")

        # avg_price 후보
        avg_price_candidates = ["avg_price", "average_price", "avg_buy_price", "entry_price", "pchs_avg_pric", "avg_cost"]
        avg_price_col = next((c for c in avg_price_candidates if c in cols), None)

        # last_price 후보
        last_price_candidates = ["last_price", "current", "current_price", "current_price_usd", "market_price", "now_price", "ovrs_now_pric", "current_px"]
        last_price_col = next((c for c in last_price_candidates if c in cols), None)

        # 날짜 컬럼 schema-aware 선택
        date_col = None
        for candidate in ["trade_date", "as_of", "as_of_date", "created_at", "updated_at"]:
            if candidate in cols:
                date_col = candidate
                break

        select_fields = ["symbol", "qty"]
        alias_map = {}
        if avg_price_col:
            select_fields.append(f"{avg_price_col} AS avg_price")
            alias_map["avg_price"] = avg_price_col
        else:
            select_fields.append("0.0 AS avg_price")
            logger.warning("[US_PNL][DB_POSITIONS][NO_AVG_PRICE_COL] none of %s found", avg_price_candidates)

        if last_price_col:
            select_fields.append(f"{last_price_col} AS last_price")
            alias_map["last_price"] = last_price_col
        else:
            select_fields.append("0.0 AS last_price")
            logger.warning("[US_PNL][DB_POSITIONS][NO_LAST_PRICE_COL] none of %s found", last_price_candidates)

        # market_value_usd, cost_usd 등 옵션 컬럼
        for opt_col in ["market_value_usd", "cost_usd", "unrealized_pnl_usd",
                        "unrealized_pnl_pct", "entry_date", "updated_at"]:
            if opt_col in cols:
                select_fields.append(opt_col)

        fields_sql = ", ".join(select_fields)

        # 날짜 WHERE 조건 동적 생성
        if date_col == "trade_date":
            where_sql = "trade_date = :td"
        elif date_col == "as_of":
            where_sql = "as_of = :td"
        elif date_col == "as_of_date":
            where_sql = "as_of_date = :td"
        elif date_col in ("created_at", "updated_at"):
            where_sql = f"{date_col}::date = :td"
        else:
            where_sql = None

        base_sql = f"SELECT {fields_sql} FROM us_positions WHERE qty > 0"
        if where_sql:
            base_sql += f" AND {where_sql}"
        base_sql += " ORDER BY symbol ASC"

        with engine.begin() as conn:
            rows = conn.execute(
                text(base_sql),
                {"td": trade_date},
            ).fetchall()
            positions = [dict(r._mapping) for r in rows]
            logger.info(
                "[US_PNL][DB_POSITIONS] count=%d date_col=%s alias=%s",
                len(positions), date_col or "none", alias_map,
            )
            return positions
    except Exception as exc:
        logger.warning("[US_PNL][DB_POSITIONS][FAIL] err=%s", exc)
        traceback.print_exc()
        return []


def _load_db_fills(engine, trade_date: str) -> list[dict]:
    """DB us_fills에서 당일 체결 조회. 컬럼 alias 자동 처리."""
    try:
        from sqlalchemy import text

        cols = _get_table_columns(engine, "us_fills")

        # filled_price 후보 (price_usd 포함)
        filled_price_candidates = ["filled_price", "fill_price", "order_price", "price_usd", "price", "ft_ccld_unpr3", "ccld_unpr"]
        filled_price_col = next((c for c in filled_price_candidates if c in cols), None)

        # qty 후보 (schema-aware)
        qty_candidates = ["qty", "filled_qty", "fill_qty", "quantity", "ft_ccld_qty", "ccld_qty"]
        qty_col = next((c for c in qty_candidates if c in cols), None)

        select_fields = ["symbol", "side"]
        if qty_col:
            select_fields.append(f"{qty_col} AS qty")
        else:
            select_fields.append("0 AS qty")
            logger.warning("[US_PNL][DB_FILLS][NO_QTY_COL] none of %s found", qty_candidates)

        if filled_price_col:
            select_fields.append(f"{filled_price_col} AS filled_price")
        else:
            select_fields.append("0.0 AS filled_price")
            logger.warning("[US_PNL][DB_FILLS][NO_FILLED_PRICE_COL] none of %s found", filled_price_candidates)

        for opt_col in ["filled_amount_usd", "filled_at", "order_id", "order_no", "client_order_key", "meta"]:
            if opt_col in cols:
                select_fields.append(opt_col)

        fields_sql = ", ".join(select_fields)

        # date column — schema-aware (us_fills uses trade_date or filled_at::date)
        fills_date_col = None
        for _dc in ["trade_date", "as_of", "as_of_date"]:
            if _dc in cols:
                fills_date_col = _dc
                break

        if fills_date_col:
            fills_where = f"{fills_date_col} = :td"
        elif "filled_at" in cols:
            fills_where = "filled_at::date = :td"
        else:
            fills_where = "1=1"  # no reliable date filter

        order_clause = " ORDER BY filled_at ASC" if "filled_at" in cols else ""
        fills_sql = f"SELECT {fields_sql} FROM us_fills WHERE {fills_where}{order_clause}"

        with engine.begin() as conn:
            rows = conn.execute(
                text(fills_sql),
                {"td": trade_date},
            ).fetchall()
            fills = [dict(r._mapping) for r in rows]
            logger.info("[US_PNL][DB_FILLS] count=%d", len(fills))
            return fills
    except Exception as exc:
        logger.warning("[US_PNL][DB_FILLS][FAIL] err=%s", exc)
        traceback.print_exc()
        return []


def _load_latest_daily_report(report_path: str | None) -> dict | None:
    """latest_us_daily_report.json 로드."""
    if not report_path:
        logger.info("[US_PNL][DAILY_REPORT][SKIP] path_not_provided")
        return None

    path = Path(report_path)
    if not path.exists():
        logger.warning("[US_PNL][DAILY_REPORT][MISSING] path=%s", path)
        return None
    
    try:
        data = json.loads(path.read_text())
        logger.info("[US_PNL][DAILY_REPORT][OK] path=%s", path)
        return data
    except Exception as exc:
        logger.warning("[US_PNL][DAILY_REPORT][FAIL] err=%s", exc)
        traceback.print_exc()
        return None


def generate_us_pnl_report(
    *,
    session: str,
    env: str,
    trade_date: str | None = None,
    output_dir: str = "reports/us_pnl",
    latest_daily_report: str | None = None,
) -> dict:
    """미국장 PnL 리포트 생성.
    
    Args:
        session: "am" | "afternoon"
        env: "practice" | "real"
        trade_date: YYYY-MM-DD, None이면 NY 기준 오늘
        output_dir: 리포트 출력 디렉토리
        latest_daily_report: latest_us_daily_report.json 경로
    
    Returns:
        {"status": "OK"|"PARTIAL"|"FAILED_PNL_REPORT", "warnings": [...], ...}
    """
    logger.info("[US_PNL][START] session=%s env=%s trade_date=%s", session, env, trade_date)
    
    if not trade_date:
        trade_date = _now_ny().strftime("%Y-%m-%d")
    
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    warnings = []
    status = "OK"
    kis_balance = None  # always defined for realized PNL extraction
    event_name = os.getenv("US_EVENT_NAME") or os.getenv("GITHUB_EVENT_NAME", "")
    run_attempt = os.getenv("US_RUN_ATTEMPT") or os.getenv("GITHUB_RUN_ATTEMPT", "")
    actor = os.getenv("US_ACTOR") or os.getenv("GITHUB_ACTOR", "")
    schedule_expected_et = os.getenv("US_SCHEDULE_EXPECTED_ET", "")
    actual_start_et = os.getenv("US_ACTUAL_START_ET", "")
    delay_seconds = _safe_int(os.getenv("US_DELAY_SECONDS", "0"))
    run_window = os.getenv("US_RUN_WINDOW", "")
    recovery_run = _safe_int(os.getenv("US_RECOVERY_RUN", "0"))
    trade_status_env = os.getenv("US_TRADE_STATUS", "")
    expected_to_trade_env = _safe_int(os.getenv("US_EXPECTED_TO_TRADE", os.getenv("EXPECTED_TO_TRADE", "0")))
    trade_runner_started_env = _safe_int(os.getenv("US_TRADE_RUNNER_STARTED", "0"))
    trade_runner_block_reason_env = os.getenv("US_TRADE_RUNNER_BLOCK_REASON", "")
    order_allowed_env = _safe_int(os.getenv("US_ORDER_ALLOWED", os.getenv("ORDER_ALLOWED", "0")))
    kis_order_allowed_env = _safe_int(os.getenv("US_KIS_ORDER_ALLOWED", os.getenv("KIS_ORDER_ALLOWED", "0")))
    orders_sent_env = _safe_int(os.getenv("US_ORDERS_SENT", "0"))
    dry_run_env = _env_bool("US_DRY_RUN", _env_bool("DRY_RUN", True))
    offline_mode_env = _env_bool("US_OFFLINE_MODE", False)
    kis_balance_status = "UNKNOWN"
    pnl_position_source = "unknown"
    kis_balance_raw_rows = 0
    kis_balance_unique_symbols = 0
    kis_balance_duplicate_rows_skipped = 0

    # ── no-trade final_status 처리 ─────────────────────────────────────
    NO_TRADE_STATUSES = {
        "FAILED_PREP_GUARD",
        "FAILED_PREP_CONTRACT",
        "SKIP_PHASE_WINDOW",
        "FAILED_DRY_RUN_CONTRACT",
        "NO_ENTRY_INTENTS",
        "OK_NO_TRADE",
    }
    
    # ── 데이터 소스 확보 ───────────────────────────────────────────────
    daily_report = _load_latest_daily_report(latest_daily_report)
    daily_report_exists = 1 if daily_report else 0
    
    # no-trade 상태이면 empty report 생성 후 바로 반환
    if daily_report and daily_report.get("final_status") in NO_TRADE_STATUSES:
        final_status = daily_report["final_status"]
        logger.info("[US_PNL][NO_TRADE] final_status=%s — generating no-trade PnL report", final_status)
        warnings.append(f"no_trade_{final_status.lower()}")
        status = "OK"
        # Skip KIS/DB fetches — report 0 positions/fills
        positions = []
        db_fills = []
        data_source = "no_trade"
        orders_sent_total = 0
    else:
        engine = _load_engine()
        
        kis_balance = None
        if env == "practice":
            kis_balance = _load_kis_balance(env)
            if kis_balance is None:
                warnings.append("kis_balance_unavailable")
                kis_balance_status = "FAILED_ALL_EXCHANGES"
            else:
                kis_balance_status = str(kis_balance.get("kis_balance_status") or "UNKNOWN")
                kis_balance_raw_rows = _safe_int(kis_balance.get("raw_count"))
                kis_balance_unique_symbols = _safe_int(kis_balance.get("unique_symbols"))
                kis_balance_duplicate_rows_skipped = _safe_int(kis_balance.get("duplicate_skipped"))
        
        db_positions = []
        db_fills = []
        if engine is not None:
            db_positions = _load_db_positions(engine, trade_date)
            db_fills = _load_db_fills(engine, trade_date)
            if not db_positions:
                warnings.append("db_positions_empty")
            if not db_fills:
                warnings.append("db_fills_empty")
        else:
            warnings.append("db_engine_unavailable")
        
        # ── positions 결정 ────────────────────────────────────────────────
        positions = []
        data_source = "unknown"
        
        if kis_balance and kis_balance.get("positions"):
            positions = kis_balance["positions"]
            data_source = "kis_balance"
            pnl_position_source = "kis_balance"
            logger.info("[US_PNL][POSITIONS][SOURCE] kis_balance count=%d", len(positions))
        elif db_positions:
            positions = db_positions
            data_source = "db_snapshot"
            pnl_position_source = "db_snapshot"
            fallback_reason = "kis_balance_unavailable" if "kis_balance_unavailable" in warnings else "kis_balance_partial"
            logger.warning("[US_PNL][POSITIONS][SOURCE] db_snapshot fallback_reason=%s", fallback_reason)
        elif daily_report and daily_report.get("positions"):
            # daily_report의 positions가 단순 개수일 수도 있으므로 주의
            pos_count = daily_report.get("positions", 0)
            if isinstance(pos_count, int):
                warnings.append("daily_report_positions_count_only")
                logger.warning("[US_PNL][POSITIONS][SOURCE] daily_report_count_only=%d", pos_count)
            else:
                positions = daily_report.get("positions", [])
                data_source = "daily_report"
                pnl_position_source = "daily_report"
                logger.info("[US_PNL][POSITIONS][SOURCE] daily_report count=%d", len(positions))
        
        if not positions:
            status = "FAILED_PNL_REPORT"
            warnings.append("missing_position_source")
            logger.warning("[US_PNL][POSITIONS][MISSING] no valid source found — generating empty report")
        elif warnings:
            status = "OK_WITH_WARNINGS"

        orders_sent_total = 0
        if daily_report:
            orders_sent_total = daily_report.get("orders_sent_total", 0)
            if orders_sent_total == 0:
                orders_sent_total = daily_report.get("orders_sent", 0)
    if orders_sent_env > 0:
        orders_sent_total = orders_sent_env
    
    #  ── orders_sent_total (no-trade 브랜치에서 이미 설정됨) ──────────────────
    # (already set in the branch above — do not overwrite)
    
    # ── PnL 계산 ──────────────────────────────────────────────────────
    total_market_value_usd = 0.0
    total_cost_usd = 0.0
    unrealized_pnl_usd = 0.0
    missing_price_count = 0
    missing_symbols: list[str] = []

    position_list = []
    for pos in positions:
        sym = pos.get("symbol", "")
        qty = _safe_int(pos.get("qty") or pos.get("quantity", 0))
        avg_price = _safe_float(_first_existing(
            pos,
            ["avg_price", "average_price", "pchs_avg_pric", "frcr_pchs_avg_pric", "avg_buy_price", "avg_cost", "entry_price"],
            default=0.0,
        ))
        last_price = _safe_float(_first_existing(
            pos,
            [
                "last_price",
                "current",
                "current_price",
                "current_price_usd",
                "market_price",
                "now_price",
                "now_price2",
                "now_pric2",
                "ovrs_now_pric",
                "ovrs_now_pric1",
                "ovrs_now_pric2",
                "ovrs_prpr",
                "current_px",
            ],
            default=0.0,
        ))

        market_value_raw = _safe_float(_first_existing(
            pos,
            ["market_value_usd", "market_value", "ovrs_stck_evlu_amt", "frcr_evlu_amt2", "ovrs_evlu_amt", "evlu_amt"],
            default=0.0,
        ))
        cost_raw = _safe_float(_first_existing(
            pos,
            ["cost_usd", "cost_basis_usd", "cost", "pchs_amt", "frcr_pchs_amt", "frcr_pchs_amt1", "pchs_amt_smtl_amt", "ovrs_stck_pchs_amt"],
            default=0.0,
        ))

        # last_price를 market_value에서 역산
        if last_price <= 0 and market_value_raw > 0 and qty > 0:
            last_price = market_value_raw / qty

        if market_value_raw <= 0 and last_price > 0 and qty > 0:
            market_value_raw = last_price * qty

        if cost_raw <= 0 and avg_price > 0 and qty > 0:
            cost_raw = avg_price * qty

        # last_price=0 means price is missing — don't use as normal PNL
        price_missing = last_price <= 0.0
        if price_missing:
            missing_price_count += 1
            if sym:
                missing_symbols.append(sym)
            market_value: float | None = None
            cost = cost_raw
            pnl: float | None = None
            pnl_pct: float | None = None
            # Still accumulate cost but not market_value / pnl
            total_cost_usd += cost
        else:
            cost = cost_raw
            pnl_raw_val = market_value_raw - cost
            pnl_pct_raw = (pnl_raw_val / cost * 100.0) if cost > 0 else 0.0
            market_value = market_value_raw
            pnl = pnl_raw_val
            pnl_pct = pnl_pct_raw
            total_market_value_usd += market_value_raw
            total_cost_usd += cost
            unrealized_pnl_usd += pnl_raw_val

        position_list.append({
            "symbol": sym,
            "qty": qty,
            "avg_price": avg_price,
            "last_price": last_price if not price_missing else None,
            "current": last_price if not price_missing else None,
            "current_price_usd": last_price if not price_missing else None,
            "market_value_usd": round(market_value, 2) if market_value is not None else None,
            "cost_usd": round(cost, 2),
            "unrealized_pnl_usd": round(pnl, 2) if pnl is not None else None,
            "unrealized_pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
            "price_missing": price_missing,
        })
    
    unrealized_pnl_pct = (unrealized_pnl_usd / total_cost_usd * 100.0) if total_cost_usd > 0 else 0.0
    
    block_normal_pnl_display = missing_price_count > 0
    if missing_price_count > 0:
        if status == "OK":
            status = "OK_WITH_WARNINGS"
        warnings.append(f"PRICE_MISSING missing_count={missing_price_count} symbols={','.join(missing_symbols)}")
        logger.warning(
            "[US_PNL][PRICE_MISSING] missing_count=%d symbols=%s — status forced to PARTIAL",
            missing_price_count,
            missing_symbols,
        )

    # realized PnL: 전략 당일 DB fills 기준으로만 계산한다.
    _realized_candidates = [
        "ovrs_rlzt_pfls_amt",
        "ovrs_rlzt_pfls_amt2",
        "rlzt_pfls",
        "realized_pnl_usd",
        "tot_evlu_pfls_amt",
    ]
    realized_pnl_usd: float | None = None
    realized_pnl_available = False
    realized_pnl_source = "unavailable"
    _kis_summary = {}
    if kis_balance and isinstance(kis_balance.get("summary"), dict):
        _kis_summary = kis_balance["summary"]
    elif kis_balance and isinstance(kis_balance.get("summary"), list) and kis_balance["summary"]:
        _kis_summary = kis_balance["summary"][0] if isinstance(kis_balance["summary"][0], dict) else {}

    _realized_from_kis = None
    _realized_from_kis_field = ""
    for _rk in _realized_candidates:
        _rv = _kis_summary.get(_rk)
        if _rv not in (None, "", 0, 0.0):
            _realized_from_kis = _safe_float(_rv)
            _realized_from_kis_field = _rk
            break

    def _fill_meta(fill: dict) -> dict:
        meta = fill.get("meta") or {}
        if isinstance(meta, dict):
            return meta
        if isinstance(meta, str) and meta.strip():
            try:
                parsed = json.loads(meta)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        return {}

    def _meta_float(meta: dict, *keys: str) -> float | None:
        for key in keys:
            value = meta.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    daily_buy_tracker: dict[str, dict[str, float]] = {}
    daily_sell_tracker: dict[str, float] = {}
    realized_total = 0.0
    missing_realized_basis = False
    has_sell_fill = False
    used_fill_cost_basis = False
    used_meta_realized = False
    for fill in db_fills:
        symbol = str(fill.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        side = str(fill.get("side") or "").upper()
        qty = _safe_int(fill.get("qty"))
        price = _safe_float(_first_existing(fill, ["price_usd", "filled_price", "price"], default=0.0))
        if qty <= 0 or price <= 0:
            continue
        if side == "BUY":
            buy_row = daily_buy_tracker.setdefault(symbol, {"qty": 0.0, "cost": 0.0})
            buy_row["qty"] += qty
            buy_row["cost"] += qty * price
            continue
        if side != "SELL":
            continue
        has_sell_fill = True
        meta = _fill_meta(fill)
        meta_realized = _meta_float(meta, "realized_pnl_usd")
        if meta_realized is not None:
            realized_total += meta_realized
            used_meta_realized = True
            continue
        cost_basis = _meta_float(meta, "cost_basis_price_usd", "pre_sell_avg_cost", "pre_sell_cost_basis_price_usd")
        if cost_basis is not None and cost_basis > 0:
            realized_total += qty * (price - cost_basis)
            used_fill_cost_basis = True
            continue

        # Fallback only: same-day BUY/SELL matching when no pre-sell basis is stored.
        buy_row = daily_buy_tracker.get(symbol, {"qty": 0.0, "cost": 0.0})
        used_qty = daily_sell_tracker.get(symbol, 0.0)
        available_qty = max(0.0, buy_row.get("qty", 0.0) - used_qty)
        matched_qty = min(float(qty), available_qty)
        if matched_qty <= 0 or buy_row.get("qty", 0.0) <= 0:
            missing_realized_basis = True
            continue
        avg_cost = buy_row["cost"] / buy_row["qty"] if buy_row["qty"] > 0 else 0.0
        realized_total += matched_qty * (price - avg_cost)
        daily_sell_tracker[symbol] = used_qty + matched_qty

    if has_sell_fill and not missing_realized_basis:
        realized_pnl_available = True
        if used_meta_realized or used_fill_cost_basis:
            realized_pnl_source = "sell_fill_cost_basis"
        else:
            realized_pnl_source = "db_fills_daily"
        realized_pnl_usd = realized_total
    elif not has_sell_fill:
        realized_pnl_available = True
        realized_pnl_source = "no_sell_fills_zero_daily_realized"
        realized_pnl_usd = 0.0
    else:
        warnings.append("realized_pnl_unavailable")

    # Current unrealized + today's realized is a marked daily view, not
    # cumulative strategy PnL. Historical realized fills/baseline are not loaded
    # here, so total strategy PnL must remain explicitly unavailable.
    marked_pnl_usd = (
        unrealized_pnl_usd + (realized_pnl_usd or 0.0)
        if realized_pnl_available and not block_normal_pnl_display
        else None
    )
    total_pnl_usd = None
    total_pnl_display_policy = "unavailable_cumulative_strategy_pnl"
    total_pnl_unavailable_reason = "cumulative_realized_history_and_strategy_baseline_not_loaded"

    if kis_balance_status == "FAILED_ALL_EXCHANGES" and pnl_position_source == "db_snapshot":
        if status == "OK":
            status = "OK_WITH_WARNINGS"
        warnings.extend(["kis_balance_unavailable", "pnl_generated_from_db_snapshot"])
    elif kis_balance_status == "PARTIAL":
        if status == "OK":
            status = "OK_WITH_WARNINGS"
        warnings.append("kis_balance_partial")

    # daily trade report is authoritative; runner/workflow env is only a fallback.
    trade_status = (daily_report or {}).get("trade_status") or (daily_report or {}).get("final_status") or trade_status_env or "INIT"
    trade_runner_started = _safe_int((daily_report or {}).get("trade_runner_started", 0))
    if trade_runner_started != 1 and os.getenv("US_TRADE_RUNNER_STARTED") is not None:
        trade_runner_started = trade_runner_started_env
    trade_runner_block_reason = (
        (daily_report or {}).get("trade_runner_block_reason")
        or (daily_report or {}).get("trade_block_reason")
        or (daily_report or {}).get("reason")
        or trade_runner_block_reason_env
        or ("not_started" if not daily_report else "")
    )
    # "none" is a sentinel set by workflow when trade runner starts (prevents stale DB fallback)
    if trade_runner_block_reason == "none":
        trade_runner_block_reason = ""

    if expected_to_trade_env == 1:
        if daily_report_exists != 1:
            status = "FAILED_PNL_REPORT"
            warnings.append("daily_report_missing")
            trade_status = trade_status or "INIT"
            trade_runner_started = 0
            trade_runner_block_reason = trade_runner_block_reason or "not_started"
        elif trade_runner_started != 1:
            status = "FAILED_PNL_REPORT"
            warnings.append("trade_runner_not_started")
        elif str(trade_status) == "INIT":
            status = "FAILED_PNL_REPORT"
            warnings.append("trade_status_init")
        elif trade_runner_block_reason == "not_started":
            status = "FAILED_PNL_REPORT"
            warnings.append("trade_runner_block_reason_not_started")
    if not schedule_expected_et:
        schedule_expected_et = str((daily_report or {}).get("schedule_expected_et", ""))
    if not actual_start_et:
        actual_start_et = str((daily_report or {}).get("actual_start_et", ""))
    if delay_seconds == 0:
        delay_seconds = _safe_int((daily_report or {}).get("delay_seconds", 0))
    if not run_window:
        run_window = str((daily_report or {}).get("run_window", ""))
    if recovery_run == 0:
        recovery_run = _safe_int((daily_report or {}).get("recovery_run", 0))

    # ── report payload ────────────────────────────────────────────────
    payload = {
        "trade_date": trade_date,
        "session": session,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "actor": actor,
        "event_name": event_name,
        "env": env,
        "dry_run": dry_run_env,
        "offline_mode": offline_mode_env,
        "source": data_source,
        "status": status,
        "pnl_status": status,
        "expected_to_trade": expected_to_trade_env,
        "trade_status": trade_status,
        "trade_runner_started": trade_runner_started,
        "trade_runner_block_reason": trade_runner_block_reason,
        "order_allowed": order_allowed_env,
        "kis_order_allowed": kis_order_allowed_env,
        "pnl_position_source": pnl_position_source,
        "kis_balance_status": kis_balance_status,
        "kis_balance_raw_rows": kis_balance_raw_rows,
        "kis_balance_unique_symbols": kis_balance_unique_symbols,
        "kis_balance_duplicate_rows_skipped": kis_balance_duplicate_rows_skipped,
        "positions_count": len(positions),
        "missing_price_count": missing_price_count,
        "missing_symbols": missing_symbols,
        "block_normal_pnl_display": block_normal_pnl_display,
        "fills_count": len(db_fills),
        "orders_sent_total": orders_sent_total,
        "daily_report_exists": daily_report_exists,
        "schedule_expected_et": schedule_expected_et,
        "actual_start_et": actual_start_et,
        "delay_seconds": delay_seconds,
        "run_window": run_window,
        "recovery_run": recovery_run,
        "missed_trade_window": (daily_report or {}).get("missed_trade_window", False),
        "total_market_value_usd": round(total_market_value_usd, 2),
        "total_cost_usd": round(total_cost_usd, 2),
        "unrealized_pnl_usd": round(unrealized_pnl_usd, 2) if not block_normal_pnl_display else None,
        "unrealized_pnl_pct": round(unrealized_pnl_pct, 2) if not block_normal_pnl_display else None,
        "realized_pnl_usd": round(realized_pnl_usd, 2) if realized_pnl_available and realized_pnl_usd is not None else None,
        "realized_pnl_available": realized_pnl_available,
        "realized_pnl_source": realized_pnl_source,
        "kis_account_realized_pnl_raw": _realized_from_kis,
        "kis_account_realized_pnl_raw_field": _realized_from_kis_field,
        "marked_pnl_usd_current_plus_daily_realized": round(marked_pnl_usd, 2) if marked_pnl_usd is not None else None,
        "total_pnl_usd": None,
        "total_pnl_display_policy": total_pnl_display_policy,
        "total_pnl_unavailable_reason": total_pnl_unavailable_reason,
        "positions": position_list,
        "warnings": warnings,
        "generated_at": _now_ny().isoformat(),
    }
    
    # ── 파일 생성 ─────────────────────────────────────────────────────
    output_base = Path(output_dir)
    output_base.mkdir(parents=True, exist_ok=True)
    
    latest_json = output_base / "latest_us_pnl_report.json"
    latest_md = output_base / "latest_us_pnl_report.md"
    latest_csv = output_base / "latest_us_pnl_report.csv"
    
    # dated directory
    dated_dir = output_base / trade_date / session
    dated_dir.mkdir(parents=True, exist_ok=True)
    dated_json = dated_dir / "us_pnl_report.json"
    dated_md = dated_dir / "us_pnl_report.md"
    dated_csv = dated_dir / "us_pnl_report.csv"
    
    # ── JSON ──────────────────────────────────────────────────────────
    json_text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    latest_json.write_text(json_text)
    dated_json.write_text(json_text)
    logger.info("[US_PNL][REPORT][WRITE] json=%s", latest_json)
    logger.info("[US_PNL][REPORT][WRITE] json=%s", dated_json)
    
    # ── Markdown ──────────────────────────────────────────────────────
    md_lines = [
        f"# US Portfolio PnL Report - {trade_date}",
        "",
        f"**Session**: {session}  ",
        f"**Env**: {env}  ",
        f"**Event**: {event_name}  ",
        f"**Run ID**: {run_id}  ",
        f"**Session**: {session}  ",
        f"**Expected To Trade**: {expected_to_trade_env}  ",
        f"**Run Attempt**: {run_attempt}  ",
        f"**Status**: {status}  ",
        f"**Trade Status**: {trade_status}  ",
        f"**Trade Runner Started**: {trade_runner_started}  ",
        f"**Trade Runner Block Reason**: {trade_runner_block_reason or '-'}  ",
        f"**Order Allowed**: {order_allowed_env}  ",
        f"**KIS Order Allowed**: {kis_order_allowed_env}  ",
        f"**Daily Report Exists**: {daily_report_exists}  ",
        f"**Schedule Expected ET**: {schedule_expected_et}  ",
        f"**Actual Start ET**: {actual_start_et}  ",
        f"**Delay Seconds**: {delay_seconds}  ",
        f"**Run Window**: {run_window}  ",
        f"**PNL Data Source**: {pnl_position_source}  ",
        f"**KIS Balance Status**: {kis_balance_status}  ",
        "",
        "## Summary",
        "",
        f"- **Positions**: {len(positions)}",
        f"- **Fills**: {len(db_fills)}",
        f"- **Orders Sent Total**: {orders_sent_total}",
        f"- **KIS Balance Raw Rows**: {kis_balance_raw_rows}",
        f"- **KIS Balance Unique Symbols**: {kis_balance_unique_symbols}",
        f"- **KIS Balance Duplicate Rows Skipped**: {kis_balance_duplicate_rows_skipped}",
        f"- **Total Market Value**: {_fmt_usd(total_market_value_usd)}",
        f"- **Total Cost**: {_fmt_usd(total_cost_usd)}",
        f"- **Unrealized PnL**: {'N/A (missing prices)' if block_normal_pnl_display else f'{_fmt_usd(unrealized_pnl_usd)} ({_fmt_pct(unrealized_pnl_pct)})'}",
        f"- **Daily Realized PnL**: {_fmt_usd(realized_pnl_usd) if realized_pnl_available and realized_pnl_usd is not None else 'N/A (daily sell fill cost basis unavailable)'}",
        f"- **Marked PnL (current unrealized + today's realized; NOT cumulative)**: {_fmt_usd(marked_pnl_usd) if marked_pnl_usd is not None else 'N/A'}",
        "- **Cumulative Strategy PnL**: N/A (historical realized history / strategy baseline not loaded)",
        f"- **KIS Account Realized Raw**: {(_fmt_usd(_realized_from_kis) if _realized_from_kis is not None else 'N/A')} (raw account field, not used in strategy PnL)",
        f"- **Schedule Expected ET**: {schedule_expected_et}",
        f"- **Actual Start ET**: {actual_start_et}",
        f"- **Delay Seconds**: {delay_seconds}",
        f"- **Run Window**: {run_window}",
        f"- **Recovery Run**: {recovery_run}",
        f"- **Missed Trade Window**: {(daily_report or {}).get('missed_trade_window', False)}",
        "",
    ]

    if kis_balance_status == "FAILED_ALL_EXCHANGES" and pnl_position_source == "db_snapshot":
        md_lines.extend([
            "주의: 이 PNL은 KIS 실계좌 잔고 조회 실패로 DB snapshot 기준으로 생성되었습니다. 실계좌 기준 확정 PNL이 아닙니다.",
            "",
        ])
    
    if warnings:
        md_lines.extend([
            "## Warnings",
            "",
            *[f"- `{w}`" for w in warnings],
            "",
        ])
    
    if position_list:
        md_lines.extend([
            "## Positions",
            "",
            "| Symbol | Qty | Avg Price | Last Price | Market Value | Cost | PnL | PnL % |",
            "|--------|-----|-----------|------------|--------------|------|-----|-------|",
        ])
        for p in position_list:
            lp_str = f"{p['last_price']:.2f}" if p.get('last_price') is not None else 'N/A'
            mv_str = _fmt_usd(p['market_value_usd']) if p['market_value_usd'] is not None else 'N/A'
            pnl_str = _fmt_usd(p['unrealized_pnl_usd']) if p['unrealized_pnl_usd'] is not None else 'N/A'
            pct_str = _fmt_pct(p['unrealized_pnl_pct']) if p['unrealized_pnl_pct'] is not None else 'N/A'
            md_lines.append(
                f"| {p['symbol']} | {p['qty']} | {p['avg_price']:.2f} | {lp_str} | "
                f"{mv_str} | {_fmt_usd(p['cost_usd'])} | "
                f"{pnl_str} | {pct_str} |"
            )
        md_lines.append("")
    
    md_text = "\n".join(md_lines)
    latest_md.write_text(md_text)
    dated_md.write_text(md_text)
    logger.info("[US_PNL][REPORT][WRITE] md=%s", latest_md)
    logger.info("[US_PNL][REPORT][WRITE] md=%s", dated_md)
    
    # ── CSV ───────────────────────────────────────────────────────────
    US_PNL_CSV_FIELDNAMES = [
        "trade_date",
        "session",
        "env",
        "symbol",
        "qty",
        "avg_price",
        "last_price",
        "current",
        "current_price_usd",
        "market_value_usd",
        "cost_usd",
        "unrealized_pnl_usd",
        "unrealized_pnl_pct",
        "price_missing",
        "source",
    ]

    def _write_csv(path: Path):
        safe_rows = []
        for row in position_list:
            r = dict(row)
            r.setdefault("trade_date", trade_date)
            r.setdefault("session", session)
            r.setdefault("env", env)
            r.setdefault("source", data_source)
            r.setdefault("current", r.get("last_price"))
            r.setdefault("current_price_usd", r.get("last_price"))
            r.setdefault("price_missing", r.get("last_price") in (None, 0, 0.0))
            safe_rows.append({k: r.get(k) for k in US_PNL_CSV_FIELDNAMES})
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=US_PNL_CSV_FIELDNAMES,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(safe_rows)
    
    _write_csv(latest_csv)
    _write_csv(dated_csv)
    logger.info("[US_PNL][REPORT][WRITE] csv=%s", latest_csv)
    logger.info("[US_PNL][REPORT][WRITE] csv=%s", dated_csv)
    
    logger.info("[US_PNL][FINAL_STATUS] status=%s source=%s", status, pnl_position_source)
    logger.info("[US_PNL][DONE] status=%s warnings=%s", status, ",".join(warnings))
    return payload


def main():
    parser = argparse.ArgumentParser(description="Generate US Portfolio PnL Report")
    parser.add_argument("--session", required=True, choices=["am", "afternoon"], help="Trading session")
    parser.add_argument("--env", default="practice", choices=["practice", "real"], help="KIS environment")
    parser.add_argument("--trade-date", help="Trade date YYYY-MM-DD, default: today NY")
    parser.add_argument("--output-dir", default="reports/us_pnl", help="Output directory")
    parser.add_argument("--latest-daily-report", help="Path to latest_us_daily_report.json")
    
    args = parser.parse_args()
    
    result = generate_us_pnl_report(
        session=args.session,
        env=args.env,
        trade_date=args.trade_date,
        output_dir=args.output_dir,
        latest_daily_report=args.latest_daily_report,
    )
    
    # exit code: FAILED_PNL_REPORT는 에러지만, 파일은 생성되었으므로 0 반환
    # 완전 실패 시에만 1 반환
    if result["status"] == "FAILED_PNL_REPORT":
        logger.warning("[US_PNL][EXIT] status=FAILED_PNL_REPORT but files created, exit_code=0")
        sys.exit(0)
    else:
        logger.info("[US_PNL][EXIT] status=%s exit_code=0", result["status"])
        sys.exit(0)


if __name__ == "__main__":
    main()
