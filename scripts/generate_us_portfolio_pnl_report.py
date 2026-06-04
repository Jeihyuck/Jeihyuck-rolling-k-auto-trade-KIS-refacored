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

# ── stdlib 로깅 설정 ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
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
        last_price = _safe_float(_first_existing(enriched, ["last_price", "current", "current_price", "current_price_usd", "market_price", "now_price", "ovrs_now_pric", "ovrs_now_pric1", "prpr", "stck_prpr", "current_px"], default=0.0))
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
        ["market_value_usd", "market_value", "ovrs_stck_evlu_amt", "frcr_evlu_amt2", "evlu_amt"],
        default=0.0,
    ))
    cost_usd = _safe_float(_first_existing(
        enriched,
        ["cost_usd", "cost_basis_usd", "cost", "purchase_amount", "pchs_amt", "frcr_pchs_amt", "frcr_pchs_amt1"],
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

        # KIS 필드 정규화
        normalized = [_normalize_kis_position(p) for p in positions]
        # symbol이 비어 있거나 qty=0인 항목 필터링
        normalized = [p for p in normalized if p["symbol"] and p["qty"] > 0]

        logger.info("[US_PNL][KIS_BALANCE][OK] positions_raw=%d positions_valid=%d", len(positions), len(normalized))

        return {
            "raw": raw,
            "positions": normalized,
            "summary": raw.get("output2", {}),
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
        filled_price_candidates = ["filled_price", "fill_price", "avg_fill_price", "order_price", "price_usd", "price", "ft_ccld_unpr3", "ccld_unpr"]
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

        for opt_col in ["filled_amount_usd", "filled_at", "order_id", "order_no", "client_order_key"]:
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
        report_path = "reports/us_daily/latest_us_daily_report.json"
    
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
            logger.info("[US_PNL][POSITIONS][SOURCE] kis_balance count=%d", len(positions))
        elif db_positions:
            positions = db_positions
            data_source = "db_snapshot"
            logger.info("[US_PNL][POSITIONS][SOURCE] db_snapshot count=%d", len(positions))
        elif daily_report and daily_report.get("positions"):
            # daily_report의 positions가 단순 개수일 수도 있으므로 주의
            pos_count = daily_report.get("positions", 0)
            if isinstance(pos_count, int):
                warnings.append("daily_report_positions_count_only")
                logger.warning("[US_PNL][POSITIONS][SOURCE] daily_report_count_only=%d", pos_count)
            else:
                positions = daily_report.get("positions", [])
                data_source = "daily_report"
                logger.info("[US_PNL][POSITIONS][SOURCE] daily_report count=%d", len(positions))
        
        if not positions:
            status = "PARTIAL"
            warnings.append("missing_position_source")
            logger.warning("[US_PNL][POSITIONS][MISSING] no valid source found — generating empty report")
        elif warnings:
            status = "PARTIAL"

        orders_sent_total = 0
        if daily_report:
            orders_sent_total = daily_report.get("orders_sent_total", 0)
            if orders_sent_total == 0:
                orders_sent_total = daily_report.get("orders_sent", 0)
    
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
                "ovrs_now_pric",
                "ovrs_now_pric1",
                "current_px",
            ],
            default=0.0,
        ))

        market_value_raw = _safe_float(_first_existing(
            pos,
            ["market_value_usd", "market_value", "ovrs_stck_evlu_amt", "frcr_evlu_amt2", "evlu_amt"],
            default=0.0,
        ))
        cost_raw = _safe_float(_first_existing(
            pos,
            ["cost_usd", "cost_basis_usd", "cost", "pchs_amt", "frcr_pchs_amt", "frcr_pchs_amt1"],
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
    
    # realized PnL: KIS summary에서 우선 읽고, 없으면 계산 불가로 처리
    _realized_candidates = [
        "ovrs_rlzt_pfls_amt",
        "ovrs_rlzt_pfls_amt2",
        "rlzt_pfls",
        "realized_pnl_usd",
        "tot_evlu_pfls_amt",
    ]
    realized_pnl_usd = 0.0
    _kis_summary = {}
    if kis_balance and isinstance(kis_balance.get("summary"), dict):
        _kis_summary = kis_balance["summary"]
    elif kis_balance and isinstance(kis_balance.get("summary"), list) and kis_balance["summary"]:
        _kis_summary = kis_balance["summary"][0] if isinstance(kis_balance["summary"][0], dict) else {}

    _realized_from_kis = None
    for _rk in _realized_candidates:
        _rv = _kis_summary.get(_rk)
        if _rv not in (None, "", 0, 0.0):
            _realized_from_kis = _safe_float(_rv)
            break

    if _realized_from_kis is not None:
        realized_pnl_usd = _realized_from_kis
    else:
        # KIS realized PNL 없음 — 매도금액을 손익으로 넣지 않음
        warnings.append("realized_pnl_unavailable")
    
    total_pnl_usd = unrealized_pnl_usd + realized_pnl_usd

    # missing_price_count > 0이면 PARTIAL로 강제
    block_normal_pnl_display = missing_price_count > 0
    if missing_price_count > 0:
        if status == "OK":
            status = "PARTIAL"
        warnings.append(f"PRICE_MISSING missing_count={missing_price_count} symbols={','.join(missing_symbols)}")
        logger.warning(
            "[US_PNL][PRICE_MISSING] missing_count=%d symbols=%s — status forced to PARTIAL",
            missing_price_count,
            missing_symbols,
        )
    
    # ── report payload ────────────────────────────────────────────────
    payload = {
        "trade_date": trade_date,
        "session": session,
        "run_id": run_id,
        "env": env,
        "dry_run": os.getenv("DRY_RUN", "1") == "1",
        "source": data_source,
        "status": status,
        "positions_count": len(positions),
        "missing_price_count": missing_price_count,
        "missing_symbols": missing_symbols,
        "block_normal_pnl_display": block_normal_pnl_display,
        "fills_count": len(db_fills),
        "orders_sent_total": orders_sent_total,
        "total_market_value_usd": round(total_market_value_usd, 2),
        "total_cost_usd": round(total_cost_usd, 2),
        "unrealized_pnl_usd": round(unrealized_pnl_usd, 2) if not block_normal_pnl_display else None,
        "unrealized_pnl_pct": round(unrealized_pnl_pct, 2) if not block_normal_pnl_display else None,
        "realized_pnl_usd": round(realized_pnl_usd, 2),
        "total_pnl_usd": round(total_pnl_usd, 2),
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
        f"**Run ID**: {run_id}  ",
        f"**Status**: {status}  ",
        f"**Data Source**: {data_source}  ",
        "",
        "## Summary",
        "",
        f"- **Positions**: {len(positions)}",
        f"- **Fills**: {len(db_fills)}",
        f"- **Orders Sent Total**: {orders_sent_total}",
        f"- **Total Market Value**: {_fmt_usd(total_market_value_usd)}",
        f"- **Total Cost**: {_fmt_usd(total_cost_usd)}",
        f"- **Unrealized PnL**: {'N/A (missing prices)' if block_normal_pnl_display else f'{_fmt_usd(unrealized_pnl_usd)} ({_fmt_pct(unrealized_pnl_pct)})'}",
        f"- **Realized PnL**: {_fmt_usd(realized_pnl_usd)}",
        f"- **Total PnL**: {_fmt_usd(total_pnl_usd)}",
        "",
    ]
    
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
    
    logger.info("[US_PNL][DONE] status=%s warnings=%d", status, len(warnings))
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
