from __future__ import annotations

import logging
import os
from typing import Any

from trader.config import PAPER_MAX_CAPITAL_KRW as DEFAULT_PAPER_MAX_CAPITAL_KRW
from trader.db.engine import make_engine
from trader.db.repos import PracticeAccountResetRepo

logger = logging.getLogger(__name__)

ACCOUNT_STATE_TABLE_ORDER = PracticeAccountResetRepo.DELETE_ORDER


def resolve_reset_env() -> str:
    strategy_env = str(os.getenv("STRATEGY_ENV") or "").strip().lower()
    kis_env = str(os.getenv("KIS_ENV") or "").strip().lower()
    for env_name in (strategy_env, kis_env):
        if env_name and env_name != "practice":
            raise RuntimeError(f"Practice account reset is only allowed for env=practice, got env={env_name}")
    env = strategy_env or kis_env or "practice"
    if env != "practice":
        raise RuntimeError(f"Practice account reset is only allowed for env=practice, got env={env}")
    return env


def resolve_capital_krw() -> int:
    raw = str(os.getenv("PAPER_MAX_CAPITAL_KRW") or DEFAULT_PAPER_MAX_CAPITAL_KRW).replace(",", "").strip()
    return int(raw or DEFAULT_PAPER_MAX_CAPITAL_KRW)


def build_account_key(env: str) -> str:
    cano = str(os.getenv("CANO") or "").strip() or "unknown"
    product_code = str(os.getenv("ACNT_PRDT_CD") or "").strip() or "unknown"
    return f"{env}:{cano}:{product_code}"


def execute_practice_account_reset(*, engine=None) -> dict[str, Any]:
    env = resolve_reset_env()
    capital_krw = resolve_capital_krw()
    account_key = build_account_key(env)
    dry_run = os.getenv("RESET_PRACTICE_ACCOUNT") != "1"
    db_engine = engine or make_engine()
    repo = PracticeAccountResetRepo(db_engine)

    logger.info(
        "[ACCOUNT_RESET][START] env=%s account_key=%s capital=%s dry_run=%s",
        env,
        account_key,
        capital_krw,
        int(dry_run),
    )

    before_counts = repo.count_account_state_rows(env=env, account_key=account_key)
    for table_name in ACCOUNT_STATE_TABLE_ORDER:
        logger.info("[ACCOUNT_RESET][COUNT_BEFORE] table=%s rows=%s", table_name, int(before_counts.get(table_name, 0)))

    if dry_run:
        logger.warning("[ACCOUNT_RESET][NOOP] RESET_PRACTICE_ACCOUNT!=1 -> no changes applied")
        return {
            "performed": False,
            "env": env,
            "account_key": account_key,
            "capital_krw": capital_krw,
            "before_counts": before_counts,
            "after_counts": dict(before_counts),
        }

    try:
        cleared_counts = repo.clear_account_state(env=env, account_key=account_key)
    except Exception as exc:
        logger.exception("[ACCOUNT_RESET][FAIL] env=%s account_key=%s err=%s", env, account_key, exc)
        raise

    after_counts = repo.count_account_state_rows(env=env, account_key=account_key)
    for table_name in ACCOUNT_STATE_TABLE_ORDER:
        logger.info("[ACCOUNT_RESET][COUNT_AFTER] table=%s rows=%s", table_name, int(after_counts.get(table_name, 0)))

    remaining = {table_name: rows for table_name, rows in after_counts.items() if int(rows or 0) != 0}
    if remaining:
        raise RuntimeError(f"Practice account reset incomplete: remaining_rows={remaining}")

    repo.insert_reset_ledger_event(
        env=env,
        account_key=account_key,
        capital_krw=capital_krw,
        cleared_counts=cleared_counts,
    )
    logger.info("[ACCOUNT_RESET][LEDGER] event=PRACTICE_ACCOUNT_RESET capital=%s", capital_krw)
    logger.info("[ACCOUNT_RESET][DONE] env=%s capital=%s", env, capital_krw)
    return {
        "performed": True,
        "env": env,
        "account_key": account_key,
        "capital_krw": capital_krw,
        "before_counts": before_counts,
        "cleared_counts": cleared_counts,
        "after_counts": after_counts,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        execute_practice_account_reset()
    except Exception as exc:
        logger.exception("[ACCOUNT_RESET][EXIT_1] err=%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())