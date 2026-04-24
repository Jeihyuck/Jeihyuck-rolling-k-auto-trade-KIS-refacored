"""
scripts/backfill_position_horizon.py

기존 보유 포지션에 trade_horizon / position_book / exit_policy_family를
entry_reason / exit_policy_family / entry_date 기준으로 backfill한다.

Usage:
    python scripts/backfill_position_horizon.py [--env practice] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

# repo root 경로
here = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.join(here, "..")
sys.path.insert(0, repo_root)

from trader.db.engine import make_engine
from trader.db.schema import PBCoreSchema
from trader.pb1_engine import _classify_trade_horizon, _horizon_to_exit_family

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("backfill_position_horizon")


# ---------------------------------------------------------------------------
# 분류 규칙
# ---------------------------------------------------------------------------
_ENTRY_REASON_HORIZON_MAP: dict[str, str] = {
    "ENTRY_MOMENTUM": "DAY_PROTECT",
    "ENTRY_OPEN_PUSH": "DAY_PROTECT",
    "ENTRY_BREAKOUT": "DAY_PROTECT",  # 당일 기준 → 하단에서 날짜 보정
    "ENTRY_PULLBACK": "SWING_CARRY",
    "ENTRY_VCP": "SWING_CARRY",
    "ENTRY_MINERVINI": "SWING_CARRY",
}

_EXIT_FAMILY_HORIZON_MAP: dict[str, str] = {
    "MOMENTUM_EXIT": "DAY_PROTECT",
    "BREAKOUT_EXIT": "DAY_PROTECT",
    "PULLBACK_EXIT": "SWING_CARRY",
}

_HORIZON_BOOK_MAP: dict[str, str] = {
    "DAY_PROTECT": "TRADING_BOOK",
    "SWING_CARRY": "SWING_BOOK",
    "CORE_CARRY": "CORE_BOOK",
}


def _resolve_horizon(row: dict, today: str) -> tuple[str, str, str, str]:
    """(horizon, book, exit_family, source) 반환"""
    entry_reason = str(row.get("entry_reason") or "").strip().upper()
    exit_policy_family = str(row.get("exit_policy_family") or "").strip().upper()
    entry_date = str(row.get("entry_ts") or row.get("entry_date", ""))[:10]

    # entry_meta_json 우선 참조
    meta = row.get("entry_meta_json") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    meta_entry_reason = str(meta.get("entry_reason") or "").strip().upper() or entry_reason
    meta_exit_family = str(meta.get("exit_policy_family") or "").strip().upper() or exit_policy_family
    meta_entry_style = str(meta.get("entry_style_selected") or "").strip().upper()

    # 1. entry_reason / entry_style 기반
    effective_reason = meta_entry_style or meta_entry_reason
    horizon = _ENTRY_REASON_HORIZON_MAP.get(effective_reason, "")
    source = "entry_reason"

    # BREAKOUT: 당일이 아니면 SWING_CARRY
    if effective_reason == "ENTRY_BREAKOUT" and horizon == "DAY_PROTECT":
        if entry_date and entry_date != today:
            horizon = "SWING_CARRY"
            source = "entry_reason_breakout_old"

    # 2. entry_reason 판단 불가 → exit_policy_family 기반
    if not horizon:
        horizon = _EXIT_FAMILY_HORIZON_MAP.get(meta_exit_family, "")
        source = "exit_policy_family"

    # 3. features 기반 분류 시도
    if not horizon:
        features = {
            "entry_style_selected": effective_reason,
            "entry_reason": effective_reason,
            "score_final": float(meta.get("score_final_at_entry") or 0),
            "atr_pct": float(meta.get("atr_pct_at_entry") or 0),
        }
        horizon = _classify_trade_horizon(features)
        source = "features_classify"

    if not horizon:
        horizon = "SWING_CARRY"
        source = "default"

    book = _HORIZON_BOOK_MAP.get(horizon, "SWING_BOOK")
    new_exit_family = _horizon_to_exit_family(horizon)
    return horizon, book, new_exit_family, source


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill position trade_horizon")
    parser.add_argument("--env", default=os.getenv("STRATEGY_ENV", "practice"))
    parser.add_argument("--strategy", default="pb1_primary")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--overwrite", action="store_true", help="기존 position_meta.trade_horizon도 덮어씀")
    args = parser.parse_args()

    db_url = os.getenv("PBCORE_DB_URL") or os.getenv("DB_URL")
    if not db_url:
        logger.error("PBCORE_DB_URL or DB_URL env var required")
        sys.exit(1)

    engine = make_engine(db_url)
    schema = PBCoreSchema()
    today = datetime.now(timezone.utc).date().isoformat()

    from sqlalchemy import select, and_, update, func, text
    with engine.connect() as conn:
        rows = conn.execute(
            select(schema.positions).where(
                and_(
                    schema.positions.c.env == args.env,
                    schema.positions.c.status.in_(["open", "open_partial", None, ""]),
                )
            )
        ).mappings().all()

    logger.info("[BACKFILL][START] env=%s rows=%d dry_run=%s", args.env, len(rows), args.dry_run)

    updated = 0
    skipped = 0
    for row in rows:
        row = dict(row)
        code = row.get("code", "")
        existing_meta = row.get("position_meta") or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except Exception:
                existing_meta = {}

        # 이미 backfill된 경우 skip (unless --overwrite)
        if existing_meta.get("trade_horizon") and not args.overwrite:
            skipped += 1
            continue

        horizon, book, new_exit_family, source = _resolve_horizon(row, today)

        logger.info(
            "[POSITIONS][HORIZON_BACKFILL] code=%s trade_horizon=%s source=%s exit_policy_family=%s",
            code, horizon, source, new_exit_family,
        )

        new_meta = {
            **existing_meta,
            "trade_horizon": horizon,
            "position_book": book,
            "exit_policy_family": new_exit_family,
            "backfilled": True,
            "backfill_reason": source,
            "backfilled_at": datetime.now(timezone.utc).isoformat(),
            "tp1_done": existing_meta.get("tp1_done", row.get("tp1_done") or False),
            "tp2_done": existing_meta.get("tp2_done", row.get("tp2_done") or False),
            "initial_stop_price": (
                existing_meta.get("initial_stop_price")
                or row.get("stop_price_at_entry")
                or row.get("initial_stop")
                or row.get("stop_price")
            ),
        }

        if not args.dry_run:
            with engine.begin() as conn:
                conn.execute(
                    update(schema.positions)
                    .where(
                        and_(
                            schema.positions.c.env == args.env,
                            schema.positions.c.sid == row["sid"],
                            schema.positions.c.mode == row["mode"],
                            schema.positions.c.code == code,
                        )
                    )
                    .values(
                        position_meta=new_meta,
                        exit_policy_family=new_exit_family,
                        updated_at=func.now(),
                    )
                )
        updated += 1

    logger.info(
        "[BACKFILL][DONE] env=%s updated=%d skipped=%d dry_run=%s",
        args.env, updated, skipped, args.dry_run,
    )


if __name__ == "__main__":
    main()
