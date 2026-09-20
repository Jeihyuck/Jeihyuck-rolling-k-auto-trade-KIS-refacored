from __future__ import annotations

import json
import logging
import os

from trader.account_state import get_account_key, get_masked_account_key
from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.kis_wrapper import KisAPI
from trader.trading_epoch import active_epoch_snapshot, start_new_trading_epoch
from trader.us.data_provider import USDataProvider

logger = logging.getLogger(__name__)


def _positive_qty(value) -> int:
    try:
        return max(0, int(float(str(value or 0).replace(",", ""))))
    except Exception:
        return 0


def _kr_holdings(snapshot: dict) -> list[dict]:
    rows = (snapshot or {}).get("output1") or []
    return [row for row in rows if _positive_qty((row or {}).get("hldg_qty") or (row or {}).get("ord_psbl_qty")) > 0]


def _us_holdings(balance: dict) -> list[dict]:
    rows = (balance or {}).get("positions") or []
    out = []
    for row in rows:
        qty = _positive_qty(
            (row or {}).get("qty")
            or (row or {}).get("holding_qty")
            or (row or {}).get("hldg_qty")
            or (row or {}).get("ord_psbl_qty")
        )
        if qty > 0:
            out.append(row)
    return out


def start_new_practice_epoch(*, engine=None, kis=None, us_provider=None, reason: str | None = None) -> dict:
    if str(os.getenv("KIS_ENV") or "practice").lower() != "practice":
        raise RuntimeError("NEW_TRADING_EPOCH_ONLY_ALLOWED_FOR_PRACTICE")
    if os.getenv("START_NEW_PRACTICE_EPOCH") != "1" or os.getenv("EPOCH_RESET_CONFIRM") != "YES":
        raise RuntimeError("NEW_TRADING_EPOCH_CONFIRMATION_REQUIRED")

    db = engine or make_engine()
    run_migrations(db)

    kr = kis or KisAPI()
    account_id = get_account_key(env="practice", kis=kr)
    masked_account = get_masked_account_key(env="practice", kis=kr)

    kr_balance = kr.get_balance_cached(force=True)
    if not isinstance(kr_balance, dict) or kr_balance.get("_stub"):
        raise RuntimeError("KR_KIS_BALANCE_NOT_AUTHORITATIVE")
    kr_open = _kr_holdings(kr_balance)
    if kr_open:
        raise RuntimeError(
            "KR_KIS_ACCOUNT_NOT_FLAT:" +
            ",".join(str((row or {}).get("pdno") or (row or {}).get("code") or "") for row in kr_open)
        )

    provider = us_provider or USDataProvider(offline=False)
    us_balance = provider.get_balance(force_refresh=True)
    if not isinstance(us_balance, dict):
        raise RuntimeError("US_KIS_BALANCE_NOT_AUTHORITATIVE")
    if str(us_balance.get("balance_parse_status") or "").upper() != "OK":
        raise RuntimeError("US_KIS_BALANCE_PARSE_NOT_OK")
    us_open = _us_holdings(us_balance)
    if us_open:
        raise RuntimeError(
            "US_KIS_ACCOUNT_NOT_FLAT:" +
            ",".join(str((row or {}).get("symbol") or "") for row in us_open)
        )

    old_epoch = active_epoch_snapshot(db, env="practice", account_id=account_id)
    new_epoch_id = start_new_trading_epoch(
        db,
        env="practice",
        account_id=account_id,
        reason=reason or os.getenv("TRADING_EPOCH_REASON") or "KIS_PRACTICE_ACCOUNT_RESET",
    )
    new_epoch = active_epoch_snapshot(db, env="practice", account_id=account_id)
    result = {
        "status": "OK",
        "env": "practice",
        "masked_account": masked_account,
        "kr_holdings": 0,
        "us_holdings": 0,
        "old_trading_epoch_id": old_epoch.get("trading_epoch_id"),
        "new_trading_epoch_id": new_epoch_id,
        "active_epoch": new_epoch,
        "history_deleted": False,
    }
    logger.info("[TRADING_EPOCH][START_NEW][DONE] %s", json.dumps(result, default=str))
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = start_new_practice_epoch()
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
