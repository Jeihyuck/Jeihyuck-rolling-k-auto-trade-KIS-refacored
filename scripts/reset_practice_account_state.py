from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from trader.account_state import (
    account_reset_mode,
    env_flag,
    expected_initial_holdings,
    expected_practice_capital_krw,
    get_account_key,
    get_masked_account_key,
    resolve_account_sanity_capital_tolerance_krw,
)
from trader.db.engine import make_engine
from trader.db.repos import PracticeAccountResetRepo
from trader.kis_wrapper import KisAPI
from trader.runtime_paths import runtime_root

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
    return expected_practice_capital_krw()


def build_account_key(env: str) -> str:
    return get_account_key(env=env)


def _extract_holdings_count(snapshot: dict | None) -> int:
    rows = (snapshot or {}).get("output1") or []
    count = 0
    for row in rows:
        try:
            qty = int(float(str((row or {}).get("hldg_qty") or (row or {}).get("ord_psbl_qty") or 0).replace(",", "")))
        except Exception:
            qty = 0
        if qty > 0:
            count += 1
    return count


def _extract_cash_krw(snapshot: dict | None) -> int | None:
    output2 = (snapshot or {}).get("output2") or {}
    if isinstance(output2, list):
        output2 = output2[0] if output2 and isinstance(output2[0], dict) else {}
    if not isinstance(output2, dict):
        return None
    for key in ("ord_psbl_cash", "ord_psbl_amt", "dnca_tot_amt", "nrcvb_buy_amt"):
        raw = output2.get(key)
        if raw in (None, ""):
            continue
        try:
            return int(float(str(raw).replace(",", "")))
        except Exception:
            continue
    return None


def _is_stub_balance_snapshot(snapshot: dict | None) -> bool:
    if not isinstance(snapshot, dict):
        return False
    return bool(snapshot.get("_stub")) or str(snapshot.get("_source") or "").strip().lower() == "http_disabled_stub"


def _archive_reset_snapshot(*, env: str, masked_account: str, payload: dict[str, Any]) -> Path:
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    safe_account = masked_account.replace(":", "_")
    target = runtime_root() / "account_reset_archive" / f"{env}_{safe_account}_{stamp}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return target


def execute_practice_account_state_reset(*, engine=None, kis=None) -> dict[str, Any]:
    env = resolve_reset_env()
    capital_krw = resolve_capital_krw()
    expected_holdings = expected_initial_holdings()
    capital_tolerance = resolve_account_sanity_capital_tolerance_krw(capital_krw)
    dry_run = os.getenv("RESET_PRACTICE_ACCOUNT") != "1"
    db_engine = engine or make_engine()
    reset_repo = PracticeAccountResetRepo(db_engine)
    kis_client = kis or KisAPI()
    account_key = get_account_key(env=env, kis=kis_client)
    masked_account = get_masked_account_key(env=env, kis=kis_client)
    snapshot = kis_client.get_balance_cached(force=True)
    if _is_stub_balance_snapshot(snapshot):
        logger.error("[ACCOUNT_RESET][ABORT] reason=BALANCE_HTTP_DISABLED_OR_STUB account=%s", masked_account)
        raise RuntimeError(
            f"Reset aborted: KIS balance HTTP disabled or stub balance returned masked_account={masked_account}"
        )
    holdings_count = _extract_holdings_count(snapshot)
    cash_krw = _extract_cash_krw(snapshot)

    if expected_holdings >= 0 and holdings_count != expected_holdings and not env_flag("RESET_ALLOW_KIS_HOLDINGS", default=False):
        raise RuntimeError(
            f"Reset aborted: practice account holdings mismatch masked_account={masked_account} holdings={holdings_count} expected={expected_holdings}"
        )
    if cash_krw is None:
        raise RuntimeError(f"Reset aborted: capital unavailable masked_account={masked_account}")
    if capital_krw > 0 and abs(cash_krw - capital_krw) > capital_tolerance:
        raise RuntimeError(
            f"Reset aborted: practice account capital mismatch masked_account={masked_account} cash_krw={cash_krw} expected={capital_krw} tolerance={capital_tolerance}"
        )

    before_counts = reset_repo.count_account_state_rows(env=env, account_key=account_key)
    archive_payload = {
        "env": env,
        "account_key": account_key,
        "masked_account": masked_account,
        "capital_krw": capital_krw,
        "cash_krw": cash_krw,
        "expected_holdings": expected_holdings,
        "holdings_count": holdings_count,
        "before_counts": before_counts,
        "reset_mode": int(account_reset_mode()),
        "dry_run": int(dry_run),
    }
    archive_path = _archive_reset_snapshot(env=env, masked_account=masked_account, payload=archive_payload)
    logger.info(
        "[ACCOUNT_RESET][START] env=%s account=%s capital=%s cash=%s holdings=%s archive=%s dry_run=%s",
        env,
        masked_account,
        capital_krw,
        cash_krw,
        holdings_count,
        archive_path,
        int(dry_run),
    )

    for table_name in ACCOUNT_STATE_TABLE_ORDER:
        logger.info("[ACCOUNT_RESET][COUNT_BEFORE] table=%s rows=%s", table_name, int(before_counts.get(table_name, 0)))

    if dry_run:
        logger.warning("[ACCOUNT_RESET][NOOP] RESET_PRACTICE_ACCOUNT!=1 -> no changes applied")
        return {
            "performed": False,
            "env": env,
            "account_key": account_key,
            "masked_account": masked_account,
            "capital_krw": capital_krw,
            "cash_krw": cash_krw,
            "holdings_count": holdings_count,
            "archive_path": str(archive_path),
            "before_counts": before_counts,
            "after_counts": dict(before_counts),
        }

    cleared_counts = reset_repo.clear_account_state(env=env, account_key=account_key)
    after_counts = reset_repo.count_account_state_rows(env=env, account_key=account_key)
    for table_name in ACCOUNT_STATE_TABLE_ORDER:
        logger.info("[ACCOUNT_RESET][COUNT_AFTER] table=%s rows=%s", table_name, int(after_counts.get(table_name, 0)))

    remaining = {table_name: rows for table_name, rows in after_counts.items() if int(rows or 0) != 0}
    if remaining:
        raise RuntimeError(f"Practice account reset incomplete: remaining_rows={remaining}")

    reset_repo.insert_reset_ledger_event(
        env=env,
        account_key=account_key,
        capital_krw=capital_krw,
        cleared_counts=cleared_counts,
    )
    logger.info("[ACCOUNT_RESET][LEDGER] event=PRACTICE_ACCOUNT_RESET capital=%s", capital_krw)
    logger.info("[ACCOUNT_RESET][DONE] env=%s capital=%s archive=%s", env, capital_krw, archive_path)
    return {
        "performed": True,
        "env": env,
        "account_key": account_key,
        "masked_account": masked_account,
        "capital_krw": capital_krw,
        "cash_krw": cash_krw,
        "holdings_count": holdings_count,
        "archive_path": str(archive_path),
        "before_counts": before_counts,
        "cleared_counts": cleared_counts,
        "after_counts": after_counts,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        execute_practice_account_state_reset()
    except Exception as exc:
        logger.exception("[ACCOUNT_RESET][EXIT_1] err=%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())