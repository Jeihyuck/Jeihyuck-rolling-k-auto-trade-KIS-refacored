"""ACTIVE practice-only legacy US_STANDARD SELL scope bridge for any symbol.

An ACTIVE-practice-account/ACTIVE-trading-epoch V2 legacy holding may receive
a prospective management scope for a new SELL, without backdating its original
BUY. The immutable original BUY, prior actual SELL fills, TP stages, original
lifecycle and all order claims are verified from strictly current-epoch DB
rows. Historical KIS order-day queries are NOT a SELL prerequisite; the
current authenticated KIS account, live holdings, orderable shares and
same-day competing orders still require fresh broker GET before authorization
and immediately before HTTP. Ambiguous same-epoch historic SELL remains
fenced. No SHADOW, no blanket TRUE, no strategy bypass.

This module performs GET/SELECT only. All real order intents, claim/outbox,
risk gates, KIS submit and reconciliation stay in canonical order_router.
"""
from __future__ import annotations

import hashlib
import os
import threading
from collections import OrderedDict
from datetime import date
from typing import Any

from sqlalchemy import text

from trader.us.strategy_ownership import is_standard_symbol


def eligible_standard_symbol(symbol: str) -> bool:
    return bool(symbol and is_standard_symbol(symbol))

# Only fully KIS-verified, previous-day terminal original orders are cached.
# This never caches live holdings, orderable quantity or current-day orders.
_HISTORICAL_TERMINAL_CACHE = OrderedDict()
_HISTORICAL_TERMINAL_LOCK = threading.RLock()
_HISTORICAL_TERMINAL_MAX = 256


def _classify_immutable_source_order(
    *, classifier, get_order_day, day: str, current_trade_date: str,
    source_account_hash: str, epoch: str, contract_sha: str,
    symbol: str, order_no: str, side: str, requested_qty: int,
    cumulative_filled_qty: int,
) -> dict:
    """Reuse only exact historical terminal broker evidence under full scope.

    A current-day, OPEN/UNKNOWN or unverified order cannot be cached. The
    canonical router still requires fresh KIS balance/order GET per attempt,
    again just before native HTTP; this cache is NEVER submit authority.
    """
    day_key, today_key = str(day or "")[:10], str(current_trade_date or "")[:10]
    order_key = str(order_no or "").strip()
    requested, filled = int(requested_qty), int(cumulative_filled_qty)
    if not day_key or not today_key or not order_key or requested <= 0 or filled < 0:
        raise ValueError("US_LEGACY_HISTORICAL_ORDER_IDENTITY_INVALID")
    cacheable = (
        len(day_key) == 10 and len(today_key) == 10
        and day_key[4:5] == "-" and day_key[7:8] == "-"
        and today_key[4:5] == "-" and today_key[7:8] == "-"
        and day_key < today_key
        and bool(source_account_hash and epoch and contract_sha)
    )
    key = (source_account_hash, epoch, contract_sha, symbol, day_key,
           order_key, side, requested, filled)
    # Re-read native KIS evidence for every new SELL authorization. A historical
    # order can be corrected by the broker after a prior classification;
    # previously cached terminal proof must not authorize a later order.
    evidence = classifier(
        get_order_day(day), order_no=order_no, symbol=symbol,
        expected_qty=requested, trade_date=day, side=side,
    )
    status = evidence.get("classification") if isinstance(evidence, dict) else None
    actual_filled = evidence.get("cumulative_filled_qty") if isinstance(evidence, dict) else None
    terminal = (status == "PROVEN_FILLED" if side == "BUY"
                else status in {"PROVEN_FILLED", "PROVEN_CANCELLED"})
    return evidence


ORIGINAL_BUY_SQL = """
SELECT trade_date,order_no,qty_requested,qty_filled,status,meta,account_id,
       trading_epoch_id,portfolio_epoch_id,position_cycle_id,client_order_key
FROM us_orders WHERE symbol=:symbol AND side='BUY' AND
trading_epoch_id=:epoch AND env='practice' AND dry_run IS FALSE
ORDER BY trade_date,id
"""
FILLS_SQL = """
SELECT trade_date,order_no,side,qty,meta,account_id,
       trading_epoch_id,portfolio_epoch_id,position_cycle_id,client_order_key
FROM us_fills WHERE symbol=:symbol AND trading_epoch_id=:epoch
  AND side IN ('BUY','SELL') ORDER BY trade_date,id
"""
POSITION_SQL = """
SELECT DISTINCT ON (symbol) symbol,qty,exchange,as_of,meta,account_id,
  trading_epoch_id,portfolio_epoch_id,position_cycle_id
FROM us_positions WHERE symbol=:symbol AND trading_epoch_id=:epoch AND as_of<=:today
ORDER BY symbol,as_of DESC,created_at DESC
"""
HISTORICAL_SQL = """
SELECT trade_date,state->'lifecycle' AS lifecycle
FROM us_position_risk_state
WHERE trading_epoch_id=:epoch AND symbol=:symbol AND
  trade_date<=:today AND NULLIF(state->'lifecycle'->>'lifecycle_id','') IS NOT NULL
ORDER BY trade_date DESC LIMIT 2
"""
STAGES_SQL = """
SELECT trade_date,position_lifecycle_id,stage,stage_status
FROM us_profit_capture_lifecycle
WHERE trading_epoch_id=:epoch AND symbol=:symbol AND trade_date>=:buy_day
ORDER BY trade_date,stage
"""
HISTORICAL_PENDING_SQL = """
SELECT trade_date,order_no,qty_requested,qty_filled,status,meta,account_id
FROM us_orders WHERE symbol=:symbol AND side='SELL' AND env='practice'
AND trading_epoch_id=:epoch AND dry_run IS FALSE AND trade_date>=:buy_day
AND status NOT IN ('FILLED','RECONCILED','CANCELLED','REJECTED','BLOCKED','EXPIRED')
ORDER BY trade_date,order_no
"""
SELL_ORDERS_SQL = """
SELECT trade_date,order_no,qty_requested,qty_filled,status,meta,account_id,
       trading_epoch_id,portfolio_epoch_id,position_cycle_id,client_order_key
FROM us_orders WHERE symbol=:symbol AND trading_epoch_id=:epoch
AND side='SELL' AND env='practice' AND dry_run IS FALSE AND trade_date>=:buy_day
ORDER BY trade_date,order_no
"""


def _positive(v: Any) -> int | None:
    if type(v) is int:
        return v if v > 0 else None
    if isinstance(v, str) and v.isascii() and v.isdecimal():
        value = int(v)
        return value if value > 0 else None
    return None


def _nonnegative(v: Any) -> int | None:
    if type(v) is int:
        return v if v >= 0 else None
    if isinstance(v, str) and v.isascii() and v.isdecimal():
        return int(v)
    return None


def _json(d: Any) -> dict:
    return d if isinstance(d, dict) else {}


def _rows(conn, sql: str, params: dict) -> list[dict]:
    return [dict(row) for row in conn.execute(text(sql), params).mappings().all()]


def _row_in_active_practice_scope(
    row: dict, *, account: str, epoch: str,
) -> bool:
    """Only explicit ACTIVE account/epoch evidence, never a foreign fallback.

    Legacy DB columns may be NULL, but the signed original V2 contract and
    original runtime lifecycle must separately prove the active account and
    trading epoch. Any populated row/meta identity MUST match the active one.
    """
    if not account or not epoch:
        return False
    account_hash = hashlib.sha256(account.encode("utf-8")).hexdigest()
    for source in (row, _json(row.get("meta"))):
        for key in ("account_id", "account_key", "broker_account_hash"):
            value = source.get(key)
            if value not in (None, "") and str(value) not in (account, account_hash):
                return False
        value = source.get("trading_epoch_id")
        if value not in (None, "") and str(value) != epoch:
            return False
    return True


def _get_broker_safety(client, *, symbol: str, exchange: str,
                       expected_holding: int, minimum_orderable: int,
                       day: str) -> tuple[bool, str]:
    """Fresh native GET, no DB/cache fallback. Races are rechecked pre HTTP."""
    from trader.us.data_provider import normalize_us_order_status_row
    from scripts.us_legacy_nine_sell_readiness import _balance_symbols
    try:
        balance = client.get_us_balance(force_refresh=True)
        bpos, err = _balance_symbols(balance, symbols={symbol})
        if err or symbol not in bpos:
            return False, err or "KIS_HOLDING_UNAVAILABLE"
        p = bpos[symbol]
        if p["holding"] != expected_holding:
            return False, "KIS_LEGACY_HOLDING_CHANGED"
        if p["orderable"] < minimum_orderable:
            return False, "KIS_LEGACY_INSUFFICIENT_ORDERABLE_SHARES"
        # KIS orderability alone does NOT disprove an in-flight order/ACK.
        today_rows = client.get_us_today_orders(trade_date=day)
        if not isinstance(today_rows, list):
            return False, "KIS_CURRENT_ORDER_BOOK_INCOMPLETE"
        for raw in today_rows:
            if not isinstance(raw, dict):
                return False, "KIS_CURRENT_ORDER_ROW_INVALID"
            n = normalize_us_order_status_row(raw)
            if n.get("symbol") != symbol:
                # Unexpected unknown-symbol rows still imply an incomplete
                # broker response, not evidence to sell.
                if not n.get("symbol") or n.get("normalization_result") != "normalized":
                    return False, "KIS_ORDER_BOOK_INCOMPLETE"
                continue
            if (n.get("normalization_result") != "normalized"
                    or n.get("broker_operation") != "ORDER"
                    or n.get("status") not in {"FILLED","CANCELLED","REJECTED"}):
                return False, "KIS_COMPETING_ORDER_OR_CANCEL_UNVERIFIED"
        return True, "KIS_FRESH_HOLDING_AND_ORDER_BOOK_VERIFIED"
    except Exception as exc:
        return False, "KIS_FRESH_GET_" + type(exc).__name__.upper()


def _evidence_from_db(conn, *, symbol: str, trade_date: str,
                      active_epoch: str, raw_account: str,
                      intent: dict, current_holding_qty: int | None = None) -> tuple[dict | None,str]:
    """Read immutable original facts; no SQL UPDATE or guess from ticker."""
    from trader.us.entry_exit_contract import (
        US_ENTRY_EXIT_CONTRACT_VERSION, extract_us_entry_exit_contract,
        verify_us_entry_exit_contract,
    )

    params={"symbol": symbol, "today": trade_date, "epoch": active_epoch}
    poses=_rows(conn,POSITION_SQL,params)
    if len(poses)!=1:
        return None,"ONE_LEGACY_POSITION_NOT_PROVEN"
    pos=poses[0]
    holding=_positive(pos.get("qty"))
    if holding is None or str(pos.get("trading_epoch_id") or "")!=active_epoch:
        return None,"LEGACY_HOLDING_OR_EPOCH_UNVERIFIED"
    if current_holding_qty is not None:
        holding = _positive(current_holding_qty)
        if holding is None:
            return None,"LEGACY_CURRENT_HOLDING_UNVERIFIED"
    if not _row_in_active_practice_scope(
        pos, account=raw_account, epoch=active_epoch,
    ):
        return None,"LEGACY_POSITION_ACCOUNT_OR_EPOCH_MISMATCH"
    scoped_fields=(pos.get("account_id"),pos.get("portfolio_epoch_id"),
                   pos.get("position_cycle_id"))
    if all(value not in (None,"") for value in scoped_fields):
        return None,"NOT_AN_UNSCOPED_LEGACY_POSITION"
    if any(value not in (None,"") for value in scoped_fields):
        return None,"INCOMPLETE_PARTIALLY_SCOPED_LEGACY_POSITION"
    if any(_json(pos.get("meta")).get(field) not in (None,"") for field in (
        "position_cycle_id","portfolio_epoch_id",
    )):
        return None,"LEGACY_POSITION_META_SCOPE_CONFLICT"
    contract=extract_us_entry_exit_contract(pos,_json(pos.get("meta")))
    digest=str(_json(pos.get("meta")).get("entry_exit_contract_sha256") or "")
    if (not contract or contract.get("version")!=US_ENTRY_EXIT_CONTRACT_VERSION
            or contract.get("strategy_owner")!="US_STANDARD"
            or not verify_us_entry_exit_contract(contract)
            or contract.get("sha256")!=digest):
        return None,"FROZEN_ORIGINAL_V2_CONTRACT_UNVERIFIED"
    inmeta=_json(intent.get("meta"))
    if (str(intent.get("source_entry_contract_sha256")
            or inmeta.get("source_entry_contract_sha256") or "")!=digest
            or str(inmeta.get("source_entry_contract_version") or "")
            !=US_ENTRY_EXIT_CONTRACT_VERSION):
        return None,"SELL_NOT_FROM_FROZEN_V2_SIGNAL"
    histories=_rows(conn,HISTORICAL_SQL,params)
    if not histories:
        return None,"ORIGINAL_RUNTIME_LIFECYCLE_MISSING"
    history=_json(histories[0].get("lifecycle"))
    original_cycle=str(history.get("lifecycle_id") or "")
    opened=str(history.get("opened_trade_date") or "")
    if not (
        len(original_cycle)>=20
        and history.get("is_open") is True
        and history.get("env")=="practice"
        and history.get("strategy_owner")=="US_STANDARD"
        and history.get("trading_epoch_id")==active_epoch
        and history.get("account_id")==raw_account
        and len(opened)==10
    ):
        return None,"HISTORICAL_RUNTIME_SCOPE_UNVERIFIED"
    if _json(pos.get("meta")).get("position_lifecycle_id") not in (None, "", original_cycle):
        return None,"LEGACY_POSITION_META_SCOPE_CONFLICT"
    buys=_rows(conn,ORIGINAL_BUY_SQL,params)
    if len(buys)!=1:
        return None,"ORIGINAL_BUY_NOT_UNIQUE"
    buy=buys[0]
    if not _row_in_active_practice_scope(
        buy, account=raw_account, epoch=active_epoch,
    ):
        return None,"ORIGINAL_BUY_ACTIVE_ACCOUNT_OR_EPOCH_MISMATCH"
    bmeta=_json(buy.get("meta"))
    buy_qty=_positive(buy.get("qty_filled"))
    if (str(buy.get("trade_date"))!=opened or
            str(buy.get("status")) not in {"FILLED","RECONCILED"} or
            not buy.get("order_no") or buy_qty is None or
            buy_qty!=_positive(buy.get("qty_requested")) or
            bmeta.get("strategy_owner")!="US_STANDARD" or
            bmeta.get("entry_exit_contract_sha256")!=digest or
            bmeta.get("position_lifecycle_id")!=f"ENTRY:US_STANDARD:{symbol}"):
        return None,"IMMUTABLE_BUY_ORIGINAL_IDENTITY_UNVERIFIED"
    params["buy_day"]=opened
    # The original and forward management cycles have different meanings.
    # Prove the exact relationship; never accept a different cycle by ticker.
    source_evidence = {"original_cycle": original_cycle, "v2_sha": digest}
    forward_scope: tuple[str, str, str] | None = None

    def matches_original_or_verified_forward(row: dict) -> bool:
        nonlocal forward_scope
        meta = _json(row.get("meta"))
        life = str(meta.get("position_lifecycle_id") or "")
        if life == original_cycle:
            return True
        if forward_scope is None:
            forward_scope = _verified_prospective_legacy_scope(
                conn, evidence=source_evidence, symbol=symbol,
                raw_account=raw_account, epoch=active_epoch,
            )
        portfolio, cycle, account_hash = forward_scope
        required = {
            "symbol": symbol, "strategy_owner": "US_STANDARD",
            "us_legacy_forward_management_version": "US_LEGACY_NINE_V2_SELL_V1",
            "source_original_runtime_lifecycle_id": original_cycle,
            "source_original_buy_date": opened,
            "source_original_buy_order_no": str(buy["order_no"]),
            "source_original_entry_exit_contract_sha256": digest,
            "position_lifecycle_id": cycle, "position_cycle_id": cycle,
            "portfolio_epoch_id": portfolio, "broker_account_hash": account_hash,
            "trading_epoch_id": active_epoch,
        }
        return bool(
            all(str(meta.get(k) or "") == v for k, v in required.items())
            and meta.get("forward_management_not_original_buy") is True
            and meta.get("automated_sell_authorized") is True
            and str(row.get("trading_epoch_id") or "") == active_epoch
            and str(row.get("portfolio_epoch_id") or "") == portfolio
            and str(row.get("position_cycle_id") or "") == cycle
            and row.get("client_order_key")
        )

    fills=_rows(conn,FILLS_SQL,params)
    if (len([f for f in fills if f.get("side")=="BUY"])!=1 or
            not all(str(f.get("trade_date"))>=opened for f in fills)):
        return None,"LEGACY_FILL_ORIGIN_INCOMPLETE"
    net=0
    fill_sells={}
    for f in fills:
        if not _row_in_active_practice_scope(
            f, account=raw_account, epoch=active_epoch,
        ):
            return None,"ORIGINAL_FILL_ACTIVE_ACCOUNT_OR_EPOCH_MISMATCH"
        fm=_json(f.get("meta"))
        qty=_positive(f.get("qty"))
        if (qty is None or not f.get("order_no")
                or fm.get("fill_evidence_type")!="KIS_ORDER_CUMULATIVE_ACTUAL"
                or fm.get("is_synthetic") is True
                or fm.get("strategy_owner")!="US_STANDARD"):
            return None,"NON_NATIVE_OR_AMBIGUOUS_LEGACY_FILL"
        if f["side"]=="BUY":
            if (str(f.get("order_no")).lstrip("0")!=str(buy["order_no"]).lstrip("0")
                    or qty!=buy_qty or
                    fm.get("entry_exit_contract_sha256")!=digest):
                return None,"ORIGINAL_BUY_FILL_CONFLICT"
            net+=qty
        elif f["side"]=="SELL":
            if not matches_original_or_verified_forward(f):
                return None,"SELL_OTHER_RUNTIME_LIFECYCLE"
            key=(str(f["trade_date"]),str(f["order_no"]).lstrip("0"))
            if key in fill_sells:
                return None,"DUPLICATE_SELL_FILL_ORIGINAL_KEY"
            fill_sells[key]=(qty, f)
            net-=qty
        else:
            return None,"UNSUPPORTED_FILL_SIDE"
    if net!=holding:
        return None,"ORIGINAL_FILL_NET_NOT_CURRENT_HOLDING"
    sells=_rows(conn,SELL_ORDERS_SQL,params)
    sale_proofs=[]
    zero_fill_sell_proofs=[]
    matched_sells=set()
    seen_orders=set()
    for order in sells:
        if not _row_in_active_practice_scope(
            order, account=raw_account, epoch=active_epoch,
        ):
            return None,"ORIGINAL_SELL_ACTIVE_ACCOUNT_OR_EPOCH_MISMATCH"
        key=(str(order["trade_date"]),str(order.get("order_no") or "").lstrip("0"))
        if not key[1] or key in seen_orders:
            return None,"ORIGINAL_SELL_ORDER_IDENTITY_DUPLICATE"
        seen_orders.add(key)
        fill_match=fill_sells.get(key)
        qty=fill_match[0] if fill_match else None
        meta=_json(order.get("meta"))
        requested=_positive(order.get("qty_requested"))
        recorded=_nonnegative(order.get("qty_filled"))
        status=str(order.get("status") or "").upper()
        if (requested is None or recorded is None
                or recorded>requested
                or not matches_original_or_verified_forward(order)):
            return None,"ORIGINAL_SELL_DB_ORDER_FILL_CONFLICT"
        if fill_match and (
            _json(fill_match[1].get("meta")).get("position_lifecycle_id")
                != meta.get("position_lifecycle_id")
            or (meta.get("position_lifecycle_id") != original_cycle
                and fill_match[1].get("client_order_key") != order.get("client_order_key"))
        ):
            return None,"ORIGINAL_SELL_DB_ORDER_FILL_CONFLICT"
        if qty is None:
            # DB-only historical checks may accept a zero-fill terminal
            # broker observation, NEVER an OPEN/ACK/UNKNOWN or locally
            # inferred cancellation. Current live KIS safety is checked
            # independently before every SELL and again immediately pre-HTTP.
            if recorded != 0 or status not in {
                "OPEN","ACK","SUBMITTED","CANCELLED","CANCELED",
                "REJECTED","EXPIRED","BLOCKED",
            }:
                return None,"ORIGINAL_SELL_NO_FILL_UNVERIFIED"
            if (status in {"CANCELLED","CANCELED","REJECTED"}
                    and meta.get("fill_evidence_type") == "KIS_ORDER_STATUS_ACTUAL"):
                continue
            zero_fill_sell_proofs.append((key[0],str(order["order_no"]),requested,0))
            continue
        if (requested<qty or recorded!=qty
                or status not in {"FILLED","RECONCILED","CANCELLED"}):
            return None,"ORIGINAL_SELL_DB_ORDER_FILL_CONFLICT"
        matched_sells.add(key)
        sale_proofs.append((key[0],str(order["order_no"]),requested,qty))
    if matched_sells!=set(fill_sells):
        return None,"SELL_ORDER_FILL_COUNT_MISMATCH"
    stages=_rows(conn,STAGES_SQL,params)
    done=set()
    for stage in stages:
        stage_id=str(stage.get("position_lifecycle_id") or "")
        if stage_id!=original_cycle:
            return None,"ORIGINAL_TP_STAGE_CYCLE_CONFLICT"
        if str(stage.get("stage_status") or "").upper()=="DONE":
            done.add(str(stage.get("stage") or "").lower())
    requested_stage=str(inmeta.get("profit_capture_stage") or
                        intent.get("profit_capture_stage") or "").lower()
    if requested_stage and requested_stage in done:
        return None,"TP_STAGE_ALREADY_COMPLETE"
    pending=_rows(conn,HISTORICAL_PENDING_SQL,params)
    if len(pending)>100:
        return None,"HISTORICAL_SELL_PENDING_OVERFLOW"
    unresolved=[]
    for row in pending:
        if not _row_in_active_practice_scope(
            row, account=raw_account, epoch=active_epoch,
        ):
            return None,"ACTIVE_EPOCH_PENDING_SELL_ACCOUNT_MISMATCH"
        qty=_positive(row.get("qty_requested"))
        filled=_nonnegative(row.get("qty_filled"))
        if not row.get("order_no") or qty is None or filled is None:
            return None,"UNRESOLVED_LEGACY_ORDER_IDENTITY_INVALID"
        unresolved.append((str(row["trade_date"]),str(row["order_no"]),qty,filled))
    return {
        "symbol":symbol,"exchange":str(pos.get("exchange") or "").upper(),
        "holding":holding,"original_cycle":original_cycle,
        "source_buy_day":opened,"source_buy_order":str(buy["order_no"]),
        "source_buy_qty":buy_qty, "v2_sha":digest,
        "original_v2_contract":contract,
        "prior_sell_proofs":sale_proofs, "old_unresolved":unresolved,
        "zero_fill_sell_proofs":zero_fill_sell_proofs,
        "completed_tp_stages":sorted(done),
    },"DB_LEGACY_EVIDENCE_VERIFIED"


def verified_legacy_snapshot_context(conn, *, position: dict, trade_date: str,
                                     active_epoch: str) -> dict | None:
    """Bind original decision facts to a fresh broker quantity, not a portfolio.

    Used inside the authoritative snapshot transaction. The original columns
    stay unscoped; the verified historical lifecycle is a source reference.
    """
    if (os.getenv("KIS_ENV") != "practice"
            or os.getenv("WSL_RUN_MARKET", "").upper() != "US"):
        return None
    symbol = str(position.get("symbol") or "").upper()
    if not eligible_standard_symbol(symbol):
        return None
    from trader.account_state import get_account_key
    from trader.us.entry_exit_contract import US_ENTRY_EXIT_CONTRACT_VERSION
    account = get_account_key(env="practice")
    if not _row_in_active_practice_scope(position, account=account, epoch=active_epoch):
        return None
    source = _rows(conn, POSITION_SQL, {"symbol": symbol, "today": trade_date, "epoch": active_epoch})
    if len(source) != 1:
        return None
    digest = _json(source[0].get("meta")).get("entry_exit_contract_sha256")
    for obj in (position, _json(position.get("meta"))):
        if obj.get("entry_exit_contract_sha256") not in (None, "", digest):
            return None
    evidence, _ = _evidence_from_db(
        conn, symbol=symbol, trade_date=trade_date, active_epoch=active_epoch,
        raw_account=account, current_holding_qty=position.get("qty"),
        intent={"meta": {"source_entry_contract_sha256": digest,
                         "source_entry_contract_version": US_ENTRY_EXIT_CONTRACT_VERSION}},
    )
    if evidence is None:
        return None
    contract = evidence["original_v2_contract"]
    from trader.us.position_contract_provenance import restore_null_elided_policy
    for obj in (position, _json(position.get("meta"))):
        if (any(obj.get(k) not in (None, "", "US_STANDARD") for k in ("strategy_owner", "sleeve_id"))
                or obj.get("portfolio_epoch_id") or obj.get("position_cycle_id")
                or obj.get("position_lifecycle_id") not in (None, "", evidence["original_cycle"])):
            return None
        claimed = obj.get("entry_exit_contract")
        if claimed is not None and claimed != contract and restore_null_elided_policy(
            {"entry_exit_contract": claimed}, {"entry_exit_contract": contract},
        ) is None:
            return None
    management = _json(contract.get("management"))
    return {
        "strategy_owner": "US_STANDARD", "account_id": account,
        "trading_epoch_id": active_epoch,
        "position_lifecycle_id": evidence["original_cycle"],
        "entry_exit_contract": contract, "entry_exit_contract_sha256": evidence["v2_sha"],
        "entry_exit_contract_version": contract["version"],
        **{k: management[k] for k in ("book", "horizon", "exit_policy") if k in management},
        "lineage_identity_status": "VERIFIED_LEGACY_ORIGINAL",
    }



def _verified_prospective_legacy_scope(
    conn, *, evidence: dict, symbol: str, raw_account: str, epoch: str,
) -> tuple[str, str, str]:
    """One deterministic current-account/epoch management cycle.

    Shared by the DB-only stop ledger and the independent KIS-proven router.
    This forward scope is NOT backdated onto an original BUY or authorized
    merely by being computed.
    """
    from trader.us.execution.recovery_identity import current_us_portfolio_epoch
    if (not eligible_standard_symbol(symbol) or not raw_account or not epoch
            or not evidence or not evidence.get("original_cycle")
            or not evidence.get("v2_sha")):
        raise ValueError("LEGACY_FORWARD_SCOPE_UNVERIFIED")
    portfolio = current_us_portfolio_epoch(
        conn, env="practice", account_key=raw_account,
        trading_epoch_id=epoch, strategy_owner="US_STANDARD",
    )
    account_hash = hashlib.sha256(raw_account.encode()).hexdigest()
    material = "|".join((
        account_hash, epoch, portfolio, evidence["original_cycle"],
        symbol, evidence["v2_sha"], "FORWARD_ONLY_V2_LEGACY_SELL_V1",
    ))
    return portfolio, hashlib.sha256(material.encode()).hexdigest()[:32], account_hash


def build_verified_legacy_stop_obligation_position(
    position: dict, *, trade_date: str,
) -> dict:
    """Current-epoch DB-proven stop-debt scope, never a SELL authorization.

    The exit evaluator must not drop an otherwise valid stop signal simply
    because the original V2 BUY preceded a canonical portfolio epoch. Re-prove
    the DB original and current account/epoch, then persist the stop under the
    SAME prospective management cycle the canonical SELL router will derive.
    Current KIS holdings/orders are still separately checked by that router.
    """
    if (position.get("legacy_nine_decision_only_provisional") is not True
            or not eligible_standard_symbol(str(position.get("symbol") or "").upper())
            or os.getenv("KIS_ENV") != "practice"
            or os.getenv("WSL_RUN_MARKET", "").upper() != "US"
            or os.getenv("US_LEGACY_NINE_AUTO_SELL_ACTIVE", "1") != "1"):
        raise ValueError("LEGACY_STOP_PROVENANCE_NOT_ELIGIBLE")
    provenance = _json(position.get("_legacy_nine_db_original_provenance"))
    from trader.us.entry_exit_contract import US_ENTRY_EXIT_CONTRACT_VERSION
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
    from trader.account_state import get_account_key
    symbol = str(position["symbol"]).upper()
    account = get_account_key(env="practice")
    engine = _get_engine_or_none()
    if engine is None or not account:
        raise ValueError("LEGACY_STOP_DB_OR_ACCOUNT_UNAVAILABLE")
    with engine.connect() as conn:
        epoch = str(_active_us_epoch(conn, required=True))
        expected_hash = hashlib.sha256(account.encode()).hexdigest()
        if (not epoch or provenance.get("epoch") != epoch
                or provenance.get("account_hash") != expected_hash
                or provenance.get("symbol") != symbol):
            raise ValueError("LEGACY_STOP_ACTIVE_ACCOUNT_OR_EPOCH_CHANGED")
        digest = str(provenance.get("v2_sha") or "")
        intent = {
            "symbol": symbol, "side": "SELL", "strategy_owner": "US_STANDARD",
            "meta": {
                "source_entry_contract_sha256": digest,
                "source_entry_contract_version": US_ENTRY_EXIT_CONTRACT_VERSION,
            },
        }
        evidence, reason = _evidence_from_db(
            conn, symbol=symbol, trade_date=trade_date, active_epoch=epoch,
            raw_account=account, intent=intent,
        )
        if (not evidence or evidence["original_cycle"] != provenance.get("original_cycle")
                or evidence["source_buy_day"] != provenance.get("source_buy_day")
                or evidence["v2_sha"] != digest
                or evidence["holding"] != _positive(position.get("qty"))):
            raise ValueError("LEGACY_STOP_DB_ORIGINAL_UNVERIFIED:" + str(reason))
        portfolio, prospective, _ = _verified_prospective_legacy_scope(
            conn, evidence=evidence, symbol=symbol, raw_account=account,
            epoch=epoch,
        )
    scoped = dict(position)
    fields = {
        "strategy_owner": "US_STANDARD", "account_id": account,
        "trading_epoch_id": epoch, "portfolio_epoch_id": portfolio,
        "position_cycle_id": prospective, "position_lifecycle_id": prospective,
    }
    scoped.update(fields)
    scoped["meta"] = {**_json(position.get("meta")), **fields}
    scoped["_legacy_stop_proof_scoped"] = True
    return scoped


def prove_and_bind_legacy_nine_sell(
    intent: dict, *, conn: Any, kis_client: Any,
    trade_date: str, env: str, raw_account: str, active_epoch: str,
) -> tuple[bool,str,dict | None]:
    """Conditional TRUE only after scoped DB originals and fresh CURRENT KIS.

    Caller must be the canonical US_STANDARD router. Updates only the current
    SELL intent; historical BUY / fill / position rows stay immutable.
    """
    from trader.account_state import get_account_key
    import hashlib

    symbol=str(intent.get("symbol") or "").upper()
    meta=_json(intent.get("meta"))
    if (env!="practice" or os.getenv("KIS_ENV")!="practice"
            or os.getenv("WSL_RUN_MARKET","").upper()!="US"
            or str(intent.get("side") or "").upper()!="SELL"
            or str(intent.get("strategy_owner") or meta.get("strategy_owner") or "")
                .upper()!="US_STANDARD"
            or not eligible_standard_symbol(symbol)
            or not trade_date or not active_epoch or not raw_account):
        return False,"LEGACY_AUTO_SELL_SCOPE_NOT_ELIGIBLE",None
    try:
        from scripts.us_legacy_nine_originals_readonly_certify import (
            assert_practice_client_matches_current_scope,
        )
        hashed=hashlib.sha256(raw_account.encode()).hexdigest()
        assert_practice_client_matches_current_scope(
            kis_client, {"env":"practice","market":"US",
                         "account_id":hashed,"trading_epoch_id":active_epoch},
        )
        if get_account_key(env="practice")!=raw_account:
            return False,"LEGACY_ACTIVE_BROKER_ACCOUNT_MISMATCH",None
        evidence,reason=_evidence_from_db(
            conn,symbol=symbol,trade_date=trade_date,
            active_epoch=active_epoch,raw_account=raw_account,intent=intent,
        )
        if evidence is None:
            return False,reason,None
        if os.getenv("US_LEGACY_NINE_AUTO_SELL_ACTIVE","1")!="1":
            return False,"LEGACY_AUTO_SELL_EXPLICITLY_DISABLED",None
        requested=_positive(intent.get("qty"))
        if requested is None or requested>evidence["holding"]:
            return False,"LEGACY_STRATEGY_SELL_QUANTITY_INVALID",None
        if evidence["exchange"] not in {"NYSE","NASDAQ","AMEX","NASD"}:
            return False,"LEGACY_ORIGINAL_EXCHANGE_UNVERIFIED",None
        aliases={"NASD":"NASDAQ"}
        intent_exchange=aliases.get(str(intent.get("exchange") or "").upper(),
                                   str(intent.get("exchange") or "").upper())
        source_exchange=aliases.get(evidence["exchange"],evidence["exchange"])
        if intent_exchange!=source_exchange:
            return False,"LEGACY_BROKER_EXCHANGE_MISMATCH",None
        # Original BUY/fills/SELLs and immutable V2 policy were verified in
        # the ACTIVE practice account + ACTIVE trading epoch DB scope above.
        # Never requery old KIS dates, old accounts or old trading epochs.
        # A same-epoch unresolved SELL cannot be assumed cancelled merely
        # because it is old; AM/CLOSE reconciliation must resolve it first.
        if evidence["zero_fill_sell_proofs"]:
            return False,"ACTIVE_EPOCH_ZERO_FILL_SELL_UNRESOLVED",None
        if evidence["old_unresolved"]:
            return False,"ACTIVE_EPOCH_HISTORICAL_SELL_UNRESOLVED",None
        ok,reason=_get_broker_safety(
            kis_client,symbol=symbol,exchange=source_exchange,
            expected_holding=evidence["holding"],
            minimum_orderable=requested,day=trade_date,
        )
        if not ok:
            return False,reason,None
        # The SAME prospective cycle is used by the DB-verified durable
        # stop obligation; neither scope retroactively changes the source BUY.
        portfolio,prospective,_derived_account_hash=_verified_prospective_legacy_scope(
            conn,evidence=evidence,symbol=symbol,raw_account=raw_account,
            epoch=active_epoch,
        )
        if _derived_account_hash!=hashed:
            return False,"LEGACY_ACCOUNT_HASH_CONFLICT",None
        existing=str(intent.get("position_lifecycle_id") or
                     meta.get("position_lifecycle_id") or "")
        if existing not in ("",evidence["original_cycle"],prospective):
            return False,"LEGACY_ORIGINAL_POSITION_CYCLE_CONFLICT",None
        for field,expected in (
            ("broker_account_hash",hashed),("trading_epoch_id",active_epoch),
            ("portfolio_epoch_id",portfolio),("position_cycle_id",prospective),
        ):
            actual=str(intent.get(field) or meta.get(field) or "")
            if actual and actual!=expected:
                return False,"LEGACY_CURRENT_SCOPE_FIELD_CONFLICT_"+field,None
        new_meta=dict(meta)
        new_meta.update({
            "symbol":symbol,
            "strategy_owner":"US_STANDARD",
            "automated_sell_authorized":True,
            "us_legacy_forward_management_version":"US_LEGACY_NINE_V2_SELL_V1",
            "source_original_runtime_lifecycle_id":evidence["original_cycle"],
            "source_original_buy_date":evidence["source_buy_day"],
            "source_original_buy_order_no":evidence["source_buy_order"],
            "source_original_entry_exit_contract_sha256":evidence["v2_sha"],
            "legacy_existing_tp_done":evidence["completed_tp_stages"],
            "forward_management_not_original_buy":True,
            "position_lifecycle_id":prospective,
            "position_cycle_id":prospective,
            "portfolio_epoch_id":portfolio,
            "broker_account_hash":hashed,
            "trading_epoch_id":active_epoch,
        })
        intent.update({
            "meta":new_meta,"automated_sell_authorized":True,
            "position_lifecycle_id":prospective,"position_cycle_id":prospective,
            "portfolio_epoch_id":portfolio,"broker_account_hash":hashed,
            "trading_epoch_id":active_epoch,
        })
        return True,"LEGACY_FORWARD_ONLY_SELL_VERIFIED",{
            "symbol":symbol,"holding":evidence["holding"],
            "account_hash":hashed,"epoch":active_epoch,
            "source_cycle":evidence["original_cycle"],
            "prospective_cycle":prospective,
            "portfolio":portfolio,"trade_date":trade_date,
        }
    except Exception as exc:
        return False,"LEGACY_SOURCE_PROOF_"+type(exc).__name__.upper(),None


def recheck_legacy_nine_sell_before_http(
    intent: dict, *, kis_client: Any, proof: dict, qty: int,
) -> tuple[bool,str]:
    """Last fresh native broker GET; never rely on prior tick snapshot."""
    if (not proof or intent.get("automated_sell_authorized") is not True
            or _json(intent.get("meta")).get("automated_sell_authorized") is not True
            or intent.get("position_cycle_id")!=proof.get("prospective_cycle")
            or intent.get("portfolio_epoch_id")!=proof.get("portfolio")
            or intent.get("broker_account_hash")!=proof.get("account_hash")
            or _positive(qty) is None):
        return False,"LEGACY_AUTO_SELL_PROOF_CHANGED"
    return _get_broker_safety(
        kis_client,symbol=proof["symbol"],
        exchange=str(intent.get("exchange") or ""),
        expected_holding=proof["holding"],
        minimum_orderable=qty,day=proof["trade_date"],
    )



def enrich_legacy_nine_v2_decision_position(
    position: dict, *, trade_date: str,
) -> dict:
    """Restore ONLY in-memory historical V2 exit-decision inputs.

    This does not set automated_sell_authorized, create an executable order,
    backdate any current portfolio, or change a database row. A separate
    original KIS broker proof at route_order is mandatory before a SELL POST.
    It also restores completed TP-stage flags so TP1 cannot fire again
    merely because the Nov-05 fresh risk-state row is empty.
    """
    symbol=str(position.get("symbol") or "").upper()
    if (not eligible_standard_symbol(symbol) or os.getenv("KIS_ENV")!="practice"
            or os.getenv("WSL_RUN_MARKET","").upper()!="US"
            or os.getenv("US_LEGACY_NINE_AUTO_SELL_ACTIVE","1")!="1"):
        return position
    if position.get("portfolio_epoch_id") or position.get("position_cycle_id"):
        return position
    owner=str(position.get("strategy_owner") or
              _json(position.get("meta")).get("strategy_owner") or
              "US_STANDARD").upper()
    if owner!="US_STANDARD":
        return position
    try:
        from trader.us.db.repos import _get_engine_or_none,_active_us_epoch
        from trader.account_state import get_account_key
        from trader.us.entry_exit_contract import US_ENTRY_EXIT_CONTRACT_VERSION
        engine=_get_engine_or_none()
        if engine is None:
            return position
        raw_account=get_account_key(env="practice")
        with engine.connect() as conn:
            epoch=str(_active_us_epoch(conn, required=True))
            if not _row_in_active_practice_scope(position, account=raw_account, epoch=epoch):
                return position
            rows=_rows(conn,POSITION_SQL,{
                "symbol":symbol,"today":trade_date,"epoch":epoch,
            })
            if len(rows)!=1:
                return position
            meta=_json(rows[0].get("meta"))
            digest=str(meta.get("entry_exit_contract_sha256") or "")
            if not digest:
                return position
            decision_intent={
                "symbol":symbol,"side":"SELL",
                "strategy_owner":"US_STANDARD",
                "meta":{
                    "source_entry_contract_sha256":digest,
                    "source_entry_contract_version":US_ENTRY_EXIT_CONTRACT_VERSION,
                },
            }
            evidence,_=_evidence_from_db(
                conn,symbol=symbol,trade_date=trade_date,
                active_epoch=epoch,raw_account=raw_account,
                intent=decision_intent,
            )
        if not evidence or _positive(position.get("qty"))!=evidence["holding"]:
            return position
        out=dict(position)
        meta_out=dict(_json(position.get("meta")))
        old_life=evidence["original_cycle"]
        existing=str(out.get("position_lifecycle_id") or
                     meta_out.get("position_lifecycle_id") or "")
        if existing and existing!=old_life:
            return position
        source_contract=evidence["original_v2_contract"]
        existing_sha=str(meta_out.get("entry_exit_contract_sha256") or "")
        if existing_sha and existing_sha!=evidence["v2_sha"]:
            return position
        from trader.us.entry_exit_contract import verify_us_entry_exit_contract
        old_contract=meta_out.get("entry_exit_contract")
        if old_contract and (
            not verify_us_entry_exit_contract(old_contract)
            or old_contract.get("sha256")!=evidence["v2_sha"]
        ):
            return position
        out["position_lifecycle_id"]=old_life
        out["strategy_owner"]="US_STANDARD"
        out["trading_epoch_id"]=epoch
        out["account_id"]=raw_account
        out["lifecycle_identity_status"]="VERIFIED_LEGACY_ORIGINAL"
        meta_out["position_lifecycle_id"]=old_life
        meta_out["entry_exit_contract"]=source_contract
        meta_out["entry_exit_contract_sha256"]=evidence["v2_sha"]
        for stage in evidence["completed_tp_stages"]:
            if stage in {"tp1","tp2","tp3"}:
                meta_out[f"{stage}_done"]=True
        out["meta"]=meta_out
        out["legacy_nine_decision_only_provisional"]=True
        out["_legacy_nine_db_original_provenance"]={
            "symbol":symbol,"epoch":epoch,
            "account_hash":hashlib.sha256(raw_account.encode()).hexdigest(),
            "original_cycle":old_life,"v2_sha":evidence["v2_sha"],
            "source_buy_day":evidence["source_buy_day"],
        }
        return out
    except Exception:
        # Strategy may still evaluate a protective decision, but will never
        # auto route from this read-only provisional source on failure.
        return position
