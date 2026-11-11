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
    A pre-BUY position of zero and exact cumulative KIS actual fill are mandatory.
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
            order_filled = int(row.get("order_filled") if row.get("order_filled") is not None else filled)
            order_status = str(row.get("order_status") or ("FILLED" if filled == requested else "PARTIALLY_FILLED")).upper()
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
            and requested == intended and 0 < filled == cumulative == qty <= requested
            and order_filled == filled
            and (
                (order_status == "FILLED" and filled == requested)
                or (order_status == "PARTIALLY_FILLED" and filled < requested)
            )
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
    conn: Any = None,
) -> dict | None:
    """Read-only exact original BUY lookup, optionally within a snapshot transaction."""
    if not identity.get("trading_epoch_id") or not identity.get("account_id"):
        return None
    try:
        from sqlalchemy import text
        from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
        if conn is None:
            engine = _get_engine_or_none()
            if engine is None:
                return None
            with engine.connect() as scoped_connection:
                return load_verified_us_original_buy_scope(
                    symbol=symbol, trade_date=trade_date, qty=qty,
                    identity=identity, conn=scoped_connection,
                )
        active_epoch = str(_active_us_epoch(conn) or "")
        if active_epoch != str(identity["trading_epoch_id"]):
            return None
        rows = conn.execute(text("""
                SELECT o.symbol, o.trading_epoch_id, o.account_id,
                       o.portfolio_epoch_id, o.position_cycle_id,
                       o.order_no, o.client_order_key, o.qty_requested AS order_qty,
                       o.qty_filled AS order_filled, o.status AS order_status,
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
                   AND o.side='BUY' AND o.status IN ('FILLED','PARTIALLY_FILLED')
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



def verified_new_buy_snapshot_identity(
    *, conn: Any, trade_date: str, position: dict,
    trading_epoch_id: str,
) -> dict | None:
    """Attach original immutable BUY scope before first authoritative snapshot.

    No ticker-only, last-order, next-day, TQQQ, partial-fill or stale-balance
    adoption. A failure returns None and never fabricates a lifecycle.
    """
    from sqlalchemy import text
    from trader.us.strategy_ownership import owner_for_symbol, STANDARD_OWNER

    symbol = str(position.get("symbol") or "").upper().strip()
    meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
    if (
        not symbol or symbol == "TQQQ" or owner_for_symbol(symbol) != STANDARD_OWNER
        or position.get("position_lifecycle_id") or meta.get("position_lifecycle_id")
        or str(position.get("strategy_owner") or meta.get("strategy_owner") or "US_STANDARD").upper() != "US_STANDARD"
    ):
        return None
    try:
        qty = int(position.get("qty") or 0)
    except (TypeError, ValueError):
        return None
    if qty < 1 or not trading_epoch_id:
        return None
    accounts = conn.execute(text("""
        SELECT account_id FROM trading_epochs
        WHERE trading_epoch_id=:epoch AND env='practice' AND status='ACTIVE'
    """), {"epoch": trading_epoch_id}).mappings().all()
    if len(accounts) != 1 or not accounts[0].get("account_id"):
        return None
    identity = {
        "env": "practice", "account_id": str(accounts[0]["account_id"]),
        "trading_epoch_id": str(trading_epoch_id), "strategy_owner": "US_STANDARD",
    }
    proof = load_verified_us_original_buy_scope(
        symbol=symbol, trade_date=trade_date, qty=qty, identity=identity, conn=conn,
    )
    if proof is None:
        return None
    # Do not override an explicit partially bound but inconsistent snapshot.
    asserted = {**meta, **{k: v for k, v in position.items() if v is not None}}
    for key in ("account_id", "strategy_owner", "position_cycle_id",
                "position_lifecycle_id", "portfolio_epoch_id"):
        actual = asserted.get(key)
        expected = (
            identity["account_id"] if key == "account_id"
            else "US_STANDARD" if key == "strategy_owner"
            else proof["position_cycle_id"] if key == "position_cycle_id"
            else proof["position_lifecycle_id"] if key == "position_lifecycle_id"
            else proof["portfolio_epoch_id"]
        )
        if actual not in (None, "") and str(actual) != str(expected):
            return None
    return {
        "env": "practice", "account_id": identity["account_id"],
        "broker_account_hash": hashlib.sha256(identity["account_id"].encode()).hexdigest(),
        "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "position_cycle_id": proof["position_cycle_id"],
        "position_lifecycle_id": proof["position_lifecycle_id"],
        "portfolio_epoch_id": proof["portfolio_epoch_id"],
        "opened_trade_date": str(trade_date),
        "opened_at": str(proof["filled_at"]),
        "opened_at_source": "confirmed_buy_fill",
        "lineage_resolution": "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
    }
