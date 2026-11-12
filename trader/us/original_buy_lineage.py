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





def _exact_nonnegative_shares(raw: Any) -> int | None:
    """Strict share proof: reject bool, float, fractional strings and negatives."""
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw >= 0 else None
    if isinstance(raw, str) and raw.isascii() and raw.isdecimal():
        return int(raw)
    return None


def _exact_positive_shares(raw: Any) -> int | None:
    """Strict positive share proof; never truncate broker evidence."""
    number = _exact_nonnegative_shares(raw)
    return number if number is not None and number > 0 else None

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
    # A bool, float, or decimal-looking string is not a broker-proven share count.
    # Do not allow an integer cast at this execution/provenance boundary.
    exact_qty = _exact_positive_shares(qty)
    if env != "practice" or not account or not epoch or exact_qty is None:
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
        # The source row is part of immutable broker evidence. Never infer
        # missing order status/fill counts or truncate float/boolean values.
        filled = _exact_positive_shares(row.get("fill_qty"))
        requested = _exact_positive_shares(row.get("order_qty"))
        intended = _exact_positive_shares(row.get("intent_qty"))
        cumulative = _exact_positive_shares(fill_meta.get("cumulative_filled_qty"))
        pre_order_qty = _exact_nonnegative_shares(order_meta.get("pre_order_holding_qty"))
        order_filled = _exact_positive_shares(row.get("order_filled"))
        order_status = str(row.get("order_status") or "").strip().upper()
        if None in (filled, requested, intended, cumulative, pre_order_qty, order_filled):
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
            and requested == intended and 0 < filled == cumulative == exact_qty <= requested
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
                   AND f.account_id=o.account_id
                   AND f.portfolio_epoch_id=o.portfolio_epoch_id
                   AND f.position_cycle_id=o.position_cycle_id
                 WHERE o.trade_date=:td AND o.symbol=:symbol
                   AND o.side='BUY' AND o.status IN ('FILLED','PARTIALLY_FILLED')
                   AND o.trading_epoch_id=:epoch
                 LIMIT 51
            """), {"td": trade_date, "symbol": symbol,
                 "epoch": active_epoch}).mappings().all()
        # An exhausted bounded scan is not evidence of a globally unique BUY.
        # Reject instead of selecting one order from a truncated 50-row sample.
        if len(rows) >= 51:
            logger.error("[US_POSITION][ORIGINAL_BUY][AMBIGUOUS_SOURCE_WINDOW] symbol=%s", symbol)
            return None
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
    qty = _exact_positive_shares(position.get("qty"))
    if qty is None or not trading_epoch_id:
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
    # Original BUY contract is as economically important as its lifecycle ID.
    # The broker balance contains neither strategy policy nor TP thresholds;
    # never persist a verified scope that drops the signed original policy.
    source_meta = proof.get("meta") if isinstance(proof.get("meta"), dict) else {}
    signed = source_meta.get("entry_exit_contract")
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract
    if not isinstance(signed, dict) or not verify_us_entry_exit_contract(signed):
        return None
    return {
        "entry_exit_contract": signed,
        "entry_exit_contract_sha256": signed["sha256"],
        "entry_exit_contract_version": signed["version"],
        "book": (signed.get("management") or {}).get("book"),
        "horizon": (signed.get("management") or {}).get("horizon"),
        "exit_policy": (signed.get("management") or {}).get("exit_policy"),
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



def _verified_actual_cycle_fill_delta(
    rows: list[dict], *, share_delta: int, epoch: str,
    broker_hash: str, portfolio: str, cycle: str, contract_sha: str,
) -> bool:
    """Prove each intervening BUY/SELL against the ORIGINAL lifecycle.

    A same-contract ADD can increase shares without creating a second policy
    or new position cycle; a partial SELL may decrease them. Every observed
    order in the interval must have a *single* native KIS cumulative actual
    fill in this exact account/portfolio/epoch/cycle. Unknown, still-open,
    synthetic, partial-without-terminal-status, cross-lot and conflicting
    strategy/contract evidence fail closed, including net-zero round trips.
    This never recalculates a cost basis or certifies a fresh KIS balance.
    """
    if not rows or len(rows) > 200 or not contract_sha:
        return False
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract

    seen: set[str] = set()
    net_shares = 0
    for row in rows:
        if not isinstance(row, dict):
            return False
        order_meta = row.get("order_meta") if isinstance(row.get("order_meta"), dict) else {}
        fill_meta = row.get("fill_meta") if isinstance(row.get("fill_meta"), dict) else {}
        key = str(row.get("client_order_key") or "")
        requested = _exact_positive_shares(row.get("qty_requested"))
        filled_by_order = _exact_positive_shares(row.get("qty_filled"))
        filled_by_kis = _exact_positive_shares(row.get("fill_qty"))
        side = str(row.get("side") or "").upper()
        status = str(row.get("status") or "").upper()
        if (not key or key in seen or side not in {"BUY", "SELL"}
                or None in (requested, filled_by_order, filled_by_kis)):
            return False
        seen.add(key)
        # A later independently managed BUY cannot reuse this lifecycle.
        # The ADD policy itself is checked *before* KIS POST in order_router.
        if side == "BUY":
            frozen = order_meta.get("entry_exit_contract")
            if (
                str(order_meta.get("parent_entry_contract_sha256") or "") != contract_sha
                or not isinstance(frozen, dict)
                or not verify_us_entry_exit_contract(frozen)
                or frozen.get("sha256") != contract_sha
                or str(fill_meta.get("entry_exit_contract_sha256") or "") != contract_sha
            ):
                return False
        # Old SELL rows can lack a duplicate digest, but an explicit mismatch
        # is forbidden. Their cycle/account/owner/native proof is mandatory.
        if any(
            str(meta.get("entry_exit_contract_sha256")) != contract_sha
            for meta in (order_meta, fill_meta)
            if meta.get("entry_exit_contract_sha256") not in (None, "")
        ):
            return False
        if not (
            status in {"FILLED", "CANCELLED"}
            and str(row.get("order_no") or "")
            and str(row.get("fill_order_no") or "") == str(row.get("order_no") or "")
            and str(row.get("fill_side") or "").upper() == side
            and str(row.get("fill_client_order_key") or "") == key
            and str(row.get("trading_epoch_id") or "") == epoch
            and str(row.get("order_account_id") or "") == broker_hash
            and str(row.get("fill_account_id") or "") == broker_hash
            and str(row.get("order_portfolio_epoch_id") or "") == portfolio
            and str(row.get("fill_portfolio_epoch_id") or "") == portfolio
            and str(row.get("order_position_cycle_id") or "") == cycle
            and str(row.get("fill_position_cycle_id") or "") == cycle
            and str(order_meta.get("position_lifecycle_id") or "") == cycle
            and str(fill_meta.get("position_lifecycle_id") or "") == cycle
            and str(order_meta.get("strategy_owner") or "") == "US_STANDARD"
            and str(fill_meta.get("strategy_owner") or "") == "US_STANDARD"
            and str(order_meta.get("broker_account_hash") or "") == broker_hash
            and str(fill_meta.get("broker_account_hash") or "") == broker_hash
            and str(fill_meta.get("fill_evidence_type") or "") == "KIS_ORDER_CUMULATIVE_ACTUAL"
            and fill_meta.get("is_synthetic") is not True
            and filled_by_order == filled_by_kis <= requested
            and (status != "FILLED" or filled_by_kis == requested)
        ):
            return False
        net_shares += filled_by_kis if side == "BUY" else -filled_by_kis
    return net_shares == share_delta


def _verified_actual_partial_sell_continuity(
    rows: list[dict], *, sold_shares: int, epoch: str,
    broker_hash: str, portfolio: str, cycle: str,
) -> bool:
    """Legacy direct helper: do not grant continuity without a frozen source.

    Snapshot carry uses _verified_actual_cycle_fill_delta with contract SHA.
    """
    return False


def verified_carried_us_snapshot_identity(
    *, conn: Any, trade_date: str, position: dict,
    trading_epoch_id: str,
) -> dict | None:
    """Conservative cross-session continuity for an already verified open cycle.

    Broker balances do not contain a strategy owner or portfolio cycle. Carry
    those fields only when the *previous* scoped position and matching durable
    lifecycle agree, the current KIS holding has unchanged exact shares, and
    there has been no intervening US order or unresolved previous-day order.
    A re-entry, late fill, split, ambiguous account, or changed quantity must
    obtain a new independent original KIS proof, never inherit the old cycle.
    """
    from sqlalchemy import text
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract
    from trader.us.strategy_ownership import owner_for_symbol, STANDARD_OWNER

    sym = str(position.get("symbol") or "").strip().upper()
    meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
    qty = _exact_positive_shares(position.get("qty"))
    if (
        not sym or sym == "TQQQ" or qty is None
        or owner_for_symbol(sym) != STANDARD_OWNER or not trading_epoch_id
        or str(position.get("strategy_owner") or meta.get("strategy_owner") or "US_STANDARD").upper() != "US_STANDARD"
    ):
        return None
    # A position that already claims a lifecycle is handled by the normal
    # lifecycle path. A raw account key may be present even on broker rows:
    # validate it against the original account rather than discard evidence.
    if position.get("position_lifecycle_id") not in (None, "") or meta.get("position_lifecycle_id") not in (None, ""):
        return None

    # The newest observed balance is a hard boundary. If a previous
    # post-0062 day lost optional identity copies, walk back only a bounded
    # number of same-epoch snapshots and require *every* intervening positive
    # observation to match the proven source quantity and signed contract.
    # This is scoped snapshot provenance, never a tick-time order replay.
    prior = conn.execute(text("""
        SELECT as_of,qty,account_id,portfolio_epoch_id,position_cycle_id,meta
          FROM us_positions
         WHERE trading_epoch_id=:epoch AND symbol=:symbol AND as_of<:today
         ORDER BY as_of DESC LIMIT 1
    """), {"epoch":trading_epoch_id,"symbol":sym,"today":trade_date}).mappings().first()
    prior_qty = _exact_positive_shares((prior or {}).get("qty"))
    if prior_qty is None:
        # Never revive an earlier zero-share cycle.
        return None

    def _complete_source(row: dict) -> bool:
        source_meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        source_contract = source_meta.get("entry_exit_contract")
        source_cycle = str(row.get("position_cycle_id") or source_meta.get("position_cycle_id") or "")
        source_portfolio = str(row.get("portfolio_epoch_id") or source_meta.get("portfolio_epoch_id") or "")
        source_hash = str(row.get("account_id") or source_meta.get("broker_account_hash") or "")
        return bool(
            source_cycle and source_portfolio and source_hash
            and str(source_meta.get("strategy_owner") or "").upper() == "US_STANDARD"
            and str(source_meta.get("position_lifecycle_id") or "") == source_cycle
            and str(source_meta.get("position_cycle_id") or "") == source_cycle
            and str(source_meta.get("portfolio_epoch_id") or "") == source_portfolio
            and str(source_meta.get("broker_account_hash") or "") == source_hash
            and source_meta.get("lineage_resolution") in {
                "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
                "VERIFIED_CURRENT_EPOCH_CONTINUITY",
            }
            and isinstance(source_contract, dict)
            and verify_us_entry_exit_contract(source_contract)
            and str(source_meta.get("entry_exit_contract_sha256") or "") == source_contract["sha256"]
        )

    observed_after_source = []
    if not _complete_source(prior):
        # A missing copy is repairable; a positive but different count is not.
        # No bulk backfill, account guess, owner default or synthetic fill.
        if prior_qty != qty:
            return None
        older_rows = conn.execute(text("""
            SELECT as_of,qty,account_id,portfolio_epoch_id,position_cycle_id,meta
              FROM us_positions
             WHERE trading_epoch_id=:epoch AND symbol=:symbol AND as_of<:prior_date
             ORDER BY as_of DESC LIMIT 32
        """), {"epoch":trading_epoch_id, "symbol":sym,
               "prior_date":prior["as_of"]}).mappings().all() or []
        observed_after_source = [prior]
        previous_date = prior["as_of"]
        found = None
        for candidate in older_rows:
            candidate_date = candidate.get("as_of")
            # A duplicate/conflicting date or any changed share observation
            # cannot be silently bridged into one original cycle.
            if not candidate_date or candidate_date >= previous_date:
                return None
            previous_date = candidate_date
            if _exact_positive_shares(candidate.get("qty")) != qty:
                return None
            if _complete_source(candidate):
                found = candidate
                break
            observed_after_source.append(candidate)
        if found is None:
            return None
        source_meta = found.get("meta") if isinstance(found.get("meta"), dict) else {}
        source_contract = source_meta["entry_exit_contract"]
        source_cycle = str(found.get("position_cycle_id") or source_meta["position_cycle_id"])
        source_portfolio = str(found.get("portfolio_epoch_id") or source_meta["portfolio_epoch_id"])
        source_hash = str(found.get("account_id") or source_meta["broker_account_hash"])
        expected = {
            "account_id": source_hash,
            "portfolio_epoch_id": source_portfolio,
            "position_cycle_id": source_cycle,
            "strategy_owner": "US_STANDARD",
            "broker_account_hash": source_hash,
            "position_lifecycle_id": source_cycle,
            "entry_exit_contract_sha256": source_contract["sha256"],
        }
        for observed in observed_after_source:
            if _exact_positive_shares(observed.get("qty")) != qty:
                return None
            source_copy = observed.get("meta") if isinstance(observed.get("meta"), dict) else {}
            # An explicit assertion of another account/strategy/contract is a
            # conflict, not a partially missing snapshot to auto-adopt.
            for key, correct in expected.items():
                column = observed.get(key)
                stored = column if column not in (None, "") else source_copy.get(key)
                if stored not in (None, "") and str(stored) != correct:
                    # us_positions.account_id is a broker SHA256 scope,
                    # whereas lifecycle meta.account_id is the original raw
                    # account key. These representations may both be valid.
                    if not (
                        key == "account_id"
                        and hashlib.sha256(str(stored).encode()).hexdigest() == source_hash
                    ):
                        return None
            signed_copy = source_copy.get("entry_exit_contract")
            if signed_copy is not None and (
                not verify_us_entry_exit_contract(signed_copy)
                or signed_copy.get("sha256") != source_contract["sha256"]
            ):
                return None
        prior = found
        prior_qty = _exact_positive_shares(prior["qty"])
    prior_meta = prior.get("meta") if isinstance(prior.get("meta"),dict) else {}
    cycle = str(prior.get("position_cycle_id") or prior_meta.get("position_cycle_id") or "")
    portfolio = str(prior.get("portfolio_epoch_id") or prior_meta.get("portfolio_epoch_id") or "")
    # Older 0062 snapshots can have a complete signed source in meta and
    # risk-state while the separately-added hash column is NULL.  A NULL
    # *copy* is not proof that the original BUY identity was lost.
    # Never use this fallback unless all the original contract, active epoch,
    # account hash, cycle, lifecycle and intervening-trade checks below pass.
    broker_hash = str(prior.get("account_id") or prior_meta.get("broker_account_hash") or "")
    contract = prior_meta.get("entry_exit_contract")
    if not (
        cycle and portfolio and broker_hash
        and str(prior_meta.get("position_lifecycle_id") or "") == cycle
        and str(prior_meta.get("position_cycle_id") or "") == cycle
        and str(prior_meta.get("portfolio_epoch_id") or "") == portfolio
        and str(prior_meta.get("broker_account_hash") or "") == broker_hash
        and prior_meta.get("lineage_resolution") in {
            "EXACT_ORIGINAL_KIS_CUMULATIVE_FULL_BUY",
            "VERIFIED_CURRENT_EPOCH_CONTINUITY",
        }
        and isinstance(contract,dict)
        and verify_us_entry_exit_contract(contract)
        and str(prior_meta.get("entry_exit_contract_sha256") or "") == str(contract["sha256"])
    ):
        return None

    accounts = conn.execute(text("""
        SELECT account_id FROM trading_epochs
        WHERE trading_epoch_id=:epoch AND env='practice' AND status='ACTIVE'
    """), {"epoch":trading_epoch_id}).mappings().all()
    if len(accounts) != 1:
        return None
    raw_account = str(accounts[0].get("account_id") or "")
    if not raw_account or hashlib.sha256(raw_account.encode()).hexdigest() != broker_hash:
        return None
    supplied = {**meta, **{k: v for k, v in position.items() if v is not None}}
    for key, expected in (
        ("account_id", raw_account),
        ("portfolio_epoch_id", portfolio),
        ("position_cycle_id", cycle),
        ("strategy_owner", "US_STANDARD"),
        ("broker_account_hash", broker_hash),
    ):
        if supplied.get(key) not in (None, "") and str(supplied[key]) != str(expected):
            return None
    state = conn.execute(text("""
        SELECT state->'lifecycle' AS lifecycle
          FROM us_position_risk_state
         WHERE trading_epoch_id=:epoch AND symbol=:symbol AND trade_date=:date
    """), {"epoch":trading_epoch_id,"symbol":sym,"date":prior["as_of"]}).mappings().first()
    life = (state or {}).get("lifecycle")
    if not isinstance(life,dict) or not (
        life.get("is_open") is True
        and str(life.get("lifecycle_id") or "") == cycle
        and str(life.get("position_cycle_id") or "") == cycle
        and str(life.get("portfolio_epoch_id") or "") == portfolio
        and str(life.get("account_id") or "") == raw_account
        and str(life.get("trading_epoch_id") or "") == trading_epoch_id
        and str(life.get("strategy_owner") or "").upper() == "US_STANDARD"
        and str((life.get("entry_policy") or {}).get("entry_exit_contract_sha256") or "") == contract["sha256"]
    ):
        return None
    # Source-day unresolved orders invalidate every carry. Any skipped
    # snapshot requires a complete order-free interval from that source; a
    # quantity reduction is allowed only when direct native KIS fills prove it.
    # This is essential for TP1->TP2/runner continuity after partial fills.
    pending_previous = conn.execute(text("""
        SELECT COUNT(*) FROM us_orders
        WHERE trading_epoch_id=:epoch AND symbol=:symbol
          AND trade_date=:previous_date AND status NOT IN
              ('FILLED','CANCELLED','REJECTED','BLOCKED','EXPIRED')
    """), {"epoch":trading_epoch_id,"symbol":sym,
           "previous_date":prior["as_of"]}).scalar()
    if pending_previous is None or int(pending_previous) != 0:
        return None

    share_delta = qty - prior_qty
    if observed_after_source and share_delta != 0:
        # A missing-identity gap cannot prove share-changing trades across
        # that gap. Only a direct contiguous verified source may do so.
        return None
    intervening = conn.execute(text("""
        SELECT COUNT(*) FROM us_orders
        WHERE trading_epoch_id=:epoch AND symbol=:symbol
          AND trade_date>:previous_date AND trade_date<=:today
    """), {"epoch":trading_epoch_id,"symbol":sym,
           "previous_date":prior["as_of"],"today":trade_date}).scalar()
    if intervening is None or int(intervening) > 200:
        return None
    if int(intervening) == 0:
        if share_delta != 0:
            return None
    else:
        # Include ALL orders (both sides), not just matching terminal fills.
        # LEFT JOIN ensures any unknown claim/ACK or missing fill rejects
        # provenance; a duplicated fill row rejects uniqueness as well.
        rows = conn.execute(text("""
            SELECT o.trade_date, o.side, o.status, o.qty_requested,
                   o.qty_filled, o.client_order_key, o.order_no,
                   o.trading_epoch_id, o.account_id AS order_account_id,
                   o.portfolio_epoch_id AS order_portfolio_epoch_id,
                   o.position_cycle_id AS order_position_cycle_id,
                   o.meta AS order_meta, f.qty AS fill_qty,
                   f.order_no AS fill_order_no, f.side AS fill_side,
                   f.client_order_key AS fill_client_order_key,
                   f.account_id AS fill_account_id,
                   f.portfolio_epoch_id AS fill_portfolio_epoch_id,
                   f.position_cycle_id AS fill_position_cycle_id,
                   f.meta AS fill_meta
            FROM us_orders o
            LEFT JOIN us_fills f
              ON f.trade_date=o.trade_date
             AND f.symbol=o.symbol AND f.side=o.side
             AND f.order_no=o.order_no
             AND f.client_order_key=o.client_order_key
             AND f.trading_epoch_id=o.trading_epoch_id
             AND f.account_id=o.account_id
             AND f.portfolio_epoch_id=o.portfolio_epoch_id
             AND f.position_cycle_id=o.position_cycle_id
            WHERE o.trading_epoch_id=:epoch AND o.symbol=:symbol
              AND o.trade_date>:previous_date AND o.trade_date<=:today
            ORDER BY o.trade_date, o.client_order_key LIMIT 201
        """), {"epoch":trading_epoch_id,"symbol":sym,
               "previous_date":prior["as_of"],"today":trade_date}).mappings().all()
        if (
            len(rows) != int(intervening)
            or not _verified_actual_cycle_fill_delta(
                [dict(row) for row in rows], share_delta=share_delta,
                epoch=trading_epoch_id, broker_hash=broker_hash,
                portfolio=portfolio, cycle=cycle, contract_sha=contract["sha256"],
            )
        ):
            return None
    sold_shares = max(0, -share_delta)
    added_shares = max(0, share_delta)

    return {
        # Preserve signed V2/V3 at every authoritative next-day balance;
        # raw KIS balance rows cannot recreate an original BUY-time contract.
        "entry_exit_contract": contract,
        "entry_exit_contract_sha256": contract["sha256"],
        "entry_exit_contract_version": contract["version"],
        "book": (contract.get("management") or {}).get("book"),
        "horizon": (contract.get("management") or {}).get("horizon"),
        "exit_policy": (contract.get("management") or {}).get("exit_policy"),
        "env": "practice", "account_id": raw_account,
        "broker_account_hash": broker_hash,
        "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "portfolio_epoch_id": portfolio, "position_cycle_id": cycle,
        "position_lifecycle_id": cycle,
        "opened_trade_date": str(life.get("opened_trade_date") or ""),
        "opened_at": str(life.get("opened_at") or ""),
        "opened_at_source": str(life.get("opened_at_source") or ""),
        "high_watermark": life.get("high_watermark"),
        "high_watermark_at": life.get("high_watermark_at"),
        "high_watermark_source": life.get("high_watermark_source"),
        "holding_trade_days": life.get("holding_trade_days"),
        "lineage_resolution": "VERIFIED_CURRENT_EPOCH_CONTINUITY",
        "lineage_verified_partial_sell_shares": sold_shares,
        "lineage_verified_net_add_shares": added_shares,
    }
