"""Strict read-only original BUY identity bridge for KIS balance snapshots.

The original order's *hashed* broker account scope is different from the raw
account key retained by US lifecycle states. Bind the two only using SHA256,
exact original intent/order/KIS-cumulative fill and fresh authoritative balance.
Never mutate brokerage execution evidence or synthesize a fill.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any

logger = logging.getLogger(__name__)


def select_verified_us_original_buy_scope(
    rows: list[dict], *, symbol: str, qty: int, identity: dict[str, str],
) -> dict | None:
    """Return the only proven complete first BUY or fail closed.

    The source query must already restrict to the current trade date and epoch.
    A pre-BUY position of zero and exact cumulative KIS full fill are mandatory.
    Mixed portfolios, owner and account formats cannot be adopted by ticker.
    """
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract

    account = str(identity.get("account_id") or "")
    epoch = str(identity.get("trading_epoch_id") or "")
    env = str(identity.get("env") or "").lower()
    if env != "practice" or not account or not epoch or int(qty or 0) < 1:
        return None
    expected_hash = hashlib.sha256(account.encode("utf-8")).hexdigest()
    eligible: list[dict] = []
    for row in rows:
        order_meta = row.get("order_meta") if isinstance(row.get("order_meta"), dict) else {}
        intent_meta = row.get("intent_meta") if isinstance(row.get("intent_meta"), dict) else {}
        fill_meta = row.get("fill_meta") if isinstance(row.get("fill_meta"), dict) else {}
        owner = str(order_meta.get("strategy_owner") or "").upper()
        cycle = str(row.get("position_cycle_id") or "")
        portfolio = str(row.get("portfolio_epoch_id") or "")
        contract = order_meta.get("entry_exit_contract")
        try:
            filled = int(row.get("fill_qty") or 0)
            requested = int(row.get("order_qty") or 0)
            intended = int(row.get("intent_qty") or 0)
            cumulative = int(fill_meta.get("cumulative_filled_qty") or 0)
            pre_order_qty = int(order_meta.get("pre_order_holding_qty"))
        except (TypeError, ValueError):
            continue
        if not (
            row.get("symbol") == symbol
            and str(row.get("trading_epoch_id") or "") == epoch
            and str(row.get("account_id") or "") == expected_hash
            and owner == "US_STANDARD"
            and not (identity.get("strategy_owner")
                     and str(identity["strategy_owner"]).upper() != owner)
            and portfolio and cycle
            and row.get("order_no") and row.get("client_order_key")
            and row.get("filled_at") is not None
            and pre_order_qty == 0
            and filled == requested == intended == cumulative == qty
            and str(fill_meta.get("fill_evidence_type") or "") == "KIS_ORDER_CUMULATIVE_ACTUAL"
            and isinstance(contract, dict) and verify_us_entry_exit_contract(contract)
            and all(str(meta.get("broker_account_hash") or "") == expected_hash
                    for meta in (intent_meta, order_meta, fill_meta))
            and all(str(meta.get("position_lifecycle_id") or "") == cycle
                    for meta in (intent_meta, order_meta, fill_meta))
            and all(str(meta.get("position_cycle_id") or "") == cycle
                    for meta in (intent_meta, order_meta, fill_meta))
            and all(str(meta.get("portfolio_epoch_id") or "") == portfolio
                    for meta in (intent_meta, order_meta, fill_meta))
            and all(str(meta.get("entry_exit_contract_sha256") or "") == contract["sha256"]
                    for meta in (intent_meta, order_meta, fill_meta))
        ):
            continue
        eligible.append({
            "symbol": symbol, "side": "BUY", "qty": filled,
            "trading_epoch_id": epoch,
            "portfolio_epoch_id": portfolio,
            "position_cycle_id": cycle,
            "position_lifecycle_id": cycle,
            "strategy_owner": owner,
            "env": env,
            "account_id": account,
            "filled_at": row["filled_at"],
            "order_no": row["order_no"],
            "client_order_key": row["client_order_key"],
            "meta": {
                **fill_meta, "env": env, "account_id": account,
                "position_lifecycle_id": cycle, "position_cycle_id": cycle,
                "portfolio_epoch_id": portfolio, "strategy_owner": owner,
                "entry_exit_contract": contract,
                "entry_exit_contract_sha256": contract["sha256"],
                "lineage_resolution": "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
            },
        })
    # One original candidate, not one candidate per returned page.
    return eligible[0] if len(eligible) == 1 else None


def load_verified_us_original_buy_scope(
    *, symbol: str, trade_date: str, qty: int, identity: dict[str, str],
) -> dict | None:
    """Read-only DB lookup. Never mutate a current/older account's originals."""
    if not identity.get("trading_epoch_id") or not identity.get("account_id"):
        return None
    try:
        from sqlalchemy import text
        from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
        engine = _get_engine_or_none()
        if engine is None:
            return None
        with engine.connect() as conn:
            active_epoch = str(_active_us_epoch(conn) or "")
            if active_epoch != str(identity["trading_epoch_id"]):
                return None
            rows = conn.execute(text("""
                SELECT o.symbol, o.trading_epoch_id, o.account_id,
                       o.portfolio_epoch_id, o.position_cycle_id,
                       o.order_no, o.client_order_key, o.qty_requested AS order_qty,
                       i.qty AS intent_qty, f.qty AS fill_qty, f.filled_at,
                       o.meta AS order_meta, i.meta AS intent_meta, f.meta AS fill_meta
                  FROM us_orders o
                  JOIN us_order_intents i
                    ON i.trade_date=o.trade_date
                   AND i.symbol=o.symbol
                   AND i.client_order_key=o.client_order_key
                   AND i.trading_epoch_id=o.trading_epoch_id
                   AND i.account_id=o.account_id
                   AND i.portfolio_epoch_id=o.portfolio_epoch_id
                   AND i.position_cycle_id=o.position_cycle_id
                  JOIN us_fills f
                    ON f.trade_date=o.trade_date
                   AND f.symbol=o.symbol
                   AND f.side='BUY'
                   AND f.order_no=o.order_no
                   AND f.client_order_key=o.client_order_key
                   AND f.trading_epoch_id=o.trading_epoch_id
                 WHERE o.trade_date=:td AND o.symbol=:symbol
                   AND o.side='BUY' AND o.status='FILLED'
                   AND o.trading_epoch_id=:epoch
                 LIMIT 50
            """), {"td": trade_date, "symbol": symbol,
                   "epoch": active_epoch}).mappings().all()
        return select_verified_us_original_buy_scope(
            [dict(row) for row in rows], symbol=symbol, qty=qty, identity=identity,
        )
    except Exception as exc:
        # A DB outage may not turn an uncertain original into an accepted one.
        logger.warning("[US_POSITION][ORIGINAL_BUY][UNAVAILABLE] symbol=%s error=%s",
                       symbol, type(exc).__name__)
        return None
