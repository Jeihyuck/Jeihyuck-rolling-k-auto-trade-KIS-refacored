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

        logger.info("[US_PNL][KIS_BALANCE][OK] positions=%d", len(positions))

        return {
            "raw": raw,
            "positions": positions,
            "summary": raw.get("output2", {}),
        }
    except Exception as exc:
        logger.warning("[US_PNL][KIS_BALANCE][FAIL] err=%s", exc)
        traceback.print_exc()
        return None


def _load_db_positions(engine, trade_date: str) -> list[dict]:
    """DB us_positions에서 당일 포지션 조회."""
    try:
        from sqlalchemy import text
        with engine.begin() as conn:
            rows = conn.execute(
                text("""
                    SELECT symbol, qty, avg_price, last_price, market_value_usd,
                           cost_usd, unrealized_pnl_usd, unrealized_pnl_pct, entry_date,
                           updated_at
                    FROM us_positions
                    WHERE trade_date = :td
                    ORDER BY symbol ASC
                """),
                {"td": trade_date},
            ).fetchall()
            positions = [dict(r._mapping) for r in rows]
            logger.info("[US_PNL][DB_POSITIONS] count=%d", len(positions))
            return positions
    except Exception as exc:
        logger.warning("[US_PNL][DB_POSITIONS][FAIL] err=%s", exc)
        traceback.print_exc()
        return []


def _load_db_fills(engine, trade_date: str) -> list[dict]:
    """DB us_fills에서 당일 체결 조회."""
    try:
        from sqlalchemy import text
        with engine.begin() as conn:
            rows = conn.execute(
                text("""
                    SELECT symbol, side, qty, filled_price, filled_amount_usd,
                           filled_at, order_id
                    FROM us_fills
                    WHERE trade_date = :td
                    ORDER BY filled_at ASC
                """),
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
    
    position_list = []
    for pos in positions:
        sym = pos.get("symbol", "")
        qty = _safe_int(pos.get("qty") or pos.get("quantity", 0))
        avg_price = _safe_float(pos.get("avg_price") or pos.get("average_price", 0.0))
        last_price = _safe_float(pos.get("last_price") or pos.get("current_price", 0.0))
        
        market_value = qty * last_price
        cost = qty * avg_price
        pnl = market_value - cost
        pnl_pct = (pnl / cost * 100.0) if cost > 0 else 0.0
        
        total_market_value_usd += market_value
        total_cost_usd += cost
        unrealized_pnl_usd += pnl
        
        position_list.append({
            "symbol": sym,
            "qty": qty,
            "avg_price": avg_price,
            "last_price": last_price,
            "market_value_usd": round(market_value, 2),
            "cost_usd": round(cost, 2),
            "unrealized_pnl_usd": round(pnl, 2),
            "unrealized_pnl_pct": round(pnl_pct, 2),
        })
    
    unrealized_pnl_pct = (unrealized_pnl_usd / total_cost_usd * 100.0) if total_cost_usd > 0 else 0.0
    
    # realized PnL은 당일 체결 기준 (간략 계산)
    realized_pnl_usd = 0.0
    for fill in db_fills:
        if fill.get("side") == "SELL":
            realized_pnl_usd += _safe_float(fill.get("filled_amount_usd", 0.0))
    
    total_pnl_usd = unrealized_pnl_usd + realized_pnl_usd
    
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
        "fills_count": len(db_fills),
        "orders_sent_total": orders_sent_total,
        "total_market_value_usd": round(total_market_value_usd, 2),
        "total_cost_usd": round(total_cost_usd, 2),
        "unrealized_pnl_usd": round(unrealized_pnl_usd, 2),
        "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
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
        f"- **Unrealized PnL**: {_fmt_usd(unrealized_pnl_usd)} ({_fmt_pct(unrealized_pnl_pct)})",
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
            md_lines.append(
                f"| {p['symbol']} | {p['qty']} | {p['avg_price']:.2f} | {p['last_price']:.2f} | "
                f"{_fmt_usd(p['market_value_usd'])} | {_fmt_usd(p['cost_usd'])} | "
                f"{_fmt_usd(p['unrealized_pnl_usd'])} | {_fmt_pct(p['unrealized_pnl_pct'])} |"
            )
        md_lines.append("")
    
    md_text = "\n".join(md_lines)
    latest_md.write_text(md_text)
    dated_md.write_text(md_text)
    logger.info("[US_PNL][REPORT][WRITE] md=%s", latest_md)
    logger.info("[US_PNL][REPORT][WRITE] md=%s", dated_md)
    
    # ── CSV ───────────────────────────────────────────────────────────
    def _write_csv(path: Path):
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["symbol", "qty", "avg_price", "last_price", "market_value_usd",
                            "cost_usd", "unrealized_pnl_usd", "unrealized_pnl_pct"],
            )
            writer.writeheader()
            writer.writerows(position_list)
    
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
