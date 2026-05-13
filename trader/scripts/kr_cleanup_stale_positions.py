#!/usr/bin/env python3
"""
한국장 stale positions 정리 스크립트

KIS 실제 보유 0인데 DB positions/fills/ledger가 보유처럼 남아 있는
한국장 stale position을 정리한다.

사용법:
    # dry-run (기본)
    python -m trader.scripts.kr_cleanup_stale_positions

    # 실제 적용
    python -m trader.scripts.kr_cleanup_stale_positions --apply

    # 특정 env
    python -m trader.scripts.kr_cleanup_stale_positions --env practice --apply
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add repo to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root))

from trader.db.repos import PositionsRepo, LedgerEventsRepo
from trader.db.schema import get_db_engine, PBCoreSchema
from trader.kis_wrapper import KisAPI
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def _is_kr_stock_code(code: str | None) -> bool:
    """한국장 6자리 숫자 종목 코드 판별"""
    text = str(code or "").strip()
    return len(text) == 6 and text.isdigit()


def get_kis_actual_holdings(kis: KisAPI) -> set[str]:
    """
    KIS 실제 보유 종목 코드 set 반환
    
    Returns:
        set of 6-digit stock codes
    """
    try:
        balance = kis.get_balance()
        if not balance or not isinstance(balance, dict):
            logger.error("[KIS][BALANCE][FAIL] balance unavailable")
            return set()
        
        rt_cd = str(balance.get("rt_cd") or "0").strip()
        if rt_cd not in {"", "0"}:
            logger.error("[KIS][BALANCE][FAIL] rt_cd=%s", rt_cd)
            return set()
        
        output1 = balance.get("output1")
        if isinstance(output1, dict):
            output1_rows = [output1] if output1 else []
        elif isinstance(output1, list):
            output1_rows = output1
        else:
            output1_rows = []
        
        holdings_codes = set()
        for row in output1_rows:
            code = str(row.get("pdno") or "").zfill(6)
            if _is_kr_stock_code(code):
                holdings_codes.add(code)
        
        logger.info(
            "[KIS][BALANCE][OK] holdings_count=%s codes=%s",
            len(holdings_codes),
            sorted(holdings_codes),
        )
        
        return holdings_codes
    
    except Exception as exc:
        logger.exception("[KIS][BALANCE][ERROR] err=%s", exc)
        return set()


def find_stale_positions(
    engine,
    schema: PBCoreSchema,
    env: str,
    kis_actual_codes: set[str],
) -> list[dict]:
    """
    DB positions에서 stale positions 찾기
    
    Returns:
        list of position rows that are stale
    """
    import sqlalchemy as sa
    
    stmt = (
        sa.select(schema.positions)
        .where(
            sa.and_(
                schema.positions.c.env == env,
                schema.positions.c.qty > 0,
            )
        )
    )
    
    with engine.begin() as conn:
        rows = conn.execute(stmt).mappings().all()
    
    stale_positions = []
    for row in rows:
        code = str(row["code"] or "").zfill(6)
        
        # 한국장 6자리 숫자 code만 체크
        if not _is_kr_stock_code(code):
            continue
        
        # KIS에 없으면 stale
        if code not in kis_actual_codes:
            stale_positions.append(dict(row))
    
    return stale_positions


def close_stale_positions(
    engine,
    schema: PBCoreSchema,
    stale_positions: list[dict],
    dry_run: bool = True,
) -> int:
    """
    stale positions를 닫기
    
    Returns:
        closed count
    """
    import sqlalchemy as sa
    
    if not stale_positions:
        logger.info("[KR_RECONCILE][NO_STALE_POSITIONS]")
        return 0
    
    if dry_run:
        logger.info(
            "[KR_RECONCILE][DRY_RUN] stale_positions=%s codes=%s",
            len(stale_positions),
            [p["code"] for p in stale_positions],
        )
        return 0
    
    now_ts = now_kst()
    closed_count = 0
    
    with engine.begin() as conn:
        for pos in stale_positions:
            code = pos["code"]
            env = pos["env"]
            sid = pos["sid"]
            mode = pos["mode"]
            
            stmt = (
                sa.update(schema.positions)
                .where(
                    sa.and_(
                        schema.positions.c.env == env,
                        schema.positions.c.code == code,
                        schema.positions.c.sid == sid,
                        schema.positions.c.mode == mode,
                    )
                )
                .values(
                    qty=0,
                    status="STALE_LEDGER_CLOSED",
                    closed_reason="NO_KIS_HOLDING_RECONCILE",
                    closed_ts=now_ts,
                    last_reconciled_at=now_ts,
                )
            )
            
            result = conn.execute(stmt)
            if result.rowcount > 0:
                closed_count += 1
                logger.info(
                    "[KR_RECONCILE][CLOSE_STALE] code=%s sid=%s mode=%s",
                    code, sid, mode,
                )
    
    logger.info(
        "[KR_RECONCILE][APPLY] action=close_stale_positions count=%s",
        closed_count,
    )
    
    return closed_count


def append_stale_position_events(
    ledger_repo: LedgerEventsRepo,
    env: str,
    stale_positions: list[dict],
    dry_run: bool = True,
) -> int:
    """
    stale positions를 ledger_events에 기록
    
    Returns:
        event count
    """
    if not stale_positions:
        return 0
    
    if dry_run:
        logger.info(
            "[KR_RECONCILE][DRY_RUN] would_append_events=%s",
            len(stale_positions),
        )
        return 0
    
    event_count = 0
    for pos in stale_positions:
        try:
            ledger_repo.append_event(
                env=env,
                event_type="STALE_LEDGER_POSITION",
                code=pos["code"],
                qty=pos["qty"],
                side="NONE",
                ok=True,
                reasons=["NO_KIS_HOLDING_RECONCILE"],
                payload_json={
                    "source": "db_positions",
                    "kis_qty": 0,
                    "action": "close_stale_position",
                    "sid": pos["sid"],
                    "mode": pos["mode"],
                },
            )
            event_count += 1
        except Exception as exc:
            logger.exception(
                "[KR_RECONCILE][EVENT_FAIL] code=%s err=%s",
                pos["code"], exc,
            )
    
    logger.info(
        "[KR_RECONCILE][EVENTS_APPENDED] count=%s",
        event_count,
    )
    
    return event_count


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    
    parser = argparse.ArgumentParser(description="한국장 stale positions 정리")
    parser.add_argument(
        "--env",
        default="practice",
        help="env (practice/real)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제 적용 (기본은 dry-run)",
    )
    
    args = parser.parse_args()
    
    env = args.env
    dry_run = not args.apply
    
    logger.info(
        "[KR_RECONCILE][START] env=%s dry_run=%s",
        env, int(dry_run),
    )
    
    # DB engine
    db_url = os.environ.get("PBCORE_DB_URL")
    if not db_url:
        logger.error("[KR_RECONCILE][FAIL] PBCORE_DB_URL not set")
        return 1
    
    engine = get_db_engine(db_url)
    schema = PBCoreSchema.for_engine(engine)
    
    # KIS API
    kis = KisAPI(
        app_key=os.environ.get("KIS_APP_KEY"),
        app_secret=os.environ.get("KIS_APP_SECRET"),
        rest_url=os.environ.get("KIS_REST_URL"),
        cano=os.environ.get("CANO"),
        acnt_prdt_cd=os.environ.get("ACNT_PRDT_CD"),
        kis_env=env,
    )
    
    # 1. KIS 실제 보유 조회
    kis_actual_codes = get_kis_actual_holdings(kis)
    
    # 2. DB stale positions 찾기
    stale_positions = find_stale_positions(
        engine, schema, env, kis_actual_codes,
    )
    
    if not stale_positions:
        logger.info("[KR_RECONCILE][DONE] no_stale_positions")
        return 0
    
    logger.info(
        "[KR_RECONCILE][FOUND_STALE] count=%s codes=%s",
        len(stale_positions),
        [p["code"] for p in stale_positions],
    )
    
    # 3. stale positions 닫기
    closed_count = close_stale_positions(
        engine, schema, stale_positions, dry_run=dry_run,
    )
    
    # 4. ledger_events에 기록
    ledger_repo = LedgerEventsRepo(engine)
    event_count = append_stale_position_events(
        ledger_repo, env, stale_positions, dry_run=dry_run,
    )
    
    logger.info(
        "[KR_RECONCILE][DONE] closed=%s events=%s dry_run=%s",
        closed_count, event_count, int(dry_run),
    )
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
