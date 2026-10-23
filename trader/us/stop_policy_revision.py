"""Scoped, audit-preserving policy revision for *existing* PB1 positions.

The buy-time entry/exit contract remains immutable.  A revision is valid only
when an exact owner/account/epoch/lifecycle/symbol and original contract SHA
are matched by an authoritative PostgreSQL query.  Never load a revision from
an unverified position meta field or make the legacy 5%/8% contract mutable.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)
REVISION_CODE = "PB1_STOP_3_6_V1"


def scope_for_position(position: dict) -> tuple[str, str, str]:
    meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
    risk = position.get("risk_state") if isinstance(position.get("risk_state"), dict) else {}
    state = risk.get("state") if isinstance(risk.get("state"), dict) else {}
    life = state.get("lifecycle") if isinstance(state.get("lifecycle"), dict) else {}
    symbol = str(position.get("symbol") or meta.get("symbol") or "").upper().strip()
    owner = str(position.get("strategy_owner") or meta.get("strategy_owner") or life.get("strategy_owner") or "").upper().strip()
    lifecycle = str(position.get("position_lifecycle_id") or meta.get("position_lifecycle_id") or life.get("lifecycle_id") or "").strip()
    account = str(position.get("account_id") or meta.get("account_id") or life.get("account_id") or "").strip()
    if owner != "US_STANDARD" or not lifecycle or not account or not symbol or symbol == "TQQQ":
        return "", "", ""
    return symbol, lifecycle, account


def validated_revision(position: dict) -> dict:
    """An in-process DB-verified revision, with exact original-contract identity."""
    from trader.us.entry_exit_contract import extract_us_entry_exit_contract

    revision = position.get("_verified_pb1_stop_revision")
    if not isinstance(revision, dict) or revision.get("_db_verified") is not True:
        return {}
    symbol, lifecycle, account = scope_for_position(position)
    contract = extract_us_entry_exit_contract(position)
    if not symbol or not contract:
        return {}
    expected = (
        str(revision.get("symbol") or "").upper(),
        str(revision.get("lifecycle_id") or ""),
        str(revision.get("account_id") or ""),
        str(revision.get("source_contract_sha256") or ""),
        str(revision.get("strategy_owner") or ""),
    )
    actual = (symbol, lifecycle, account, str(contract.get("sha256") or ""), "US_STANDARD")
    if expected != actual or revision.get("revision_code") != REVISION_CODE:
        return {}
    try:
        soft, hard = float(revision["soft_stop"]), float(revision["hard_stop"])
    except (KeyError, ValueError, TypeError):
        return {}
    if soft != 0.03 or hard != 0.06:
        return {}
    return {"soft_stop": soft, "hard_stop": hard}


def load_approved_revisions(positions: list[dict], *, env: str | None = None) -> dict[tuple[str, str, str], dict]:
    """One batch read; missing migration or uncertain scope never authorizes override."""
    from trader.us.entry_exit_contract import extract_us_entry_exit_contract
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
    from sqlalchemy import text

    scopes = {}
    for pos in positions:
        symbol, lifecycle, account = scope_for_position(pos)
        contract = extract_us_entry_exit_contract(pos)
        if symbol and contract:
            scopes[(symbol, lifecycle, account)] = str(contract.get("sha256") or "")
    if not scopes:
        return {}
    engine = _get_engine_or_none()
    if engine is None:
        return {}
    environment = str(env or os.getenv("KIS_ENV", "practice")).strip().lower()
    if environment != "practice":
        # Policy rollout is explicitly restricted to the practice account.
        return {}
    with engine.connect() as conn:
        epoch = _active_us_epoch(conn)
        if not epoch:
            raise RuntimeError("PB1_STOP_REVISION_ACTIVE_EPOCH_MISSING")
        rows = conn.execute(text("""
            SELECT symbol,lifecycle_id,account_id,strategy_owner,
                   source_contract_sha256,revision_code,soft_stop,hard_stop
            FROM us_stop_policy_revisions
            WHERE trading_epoch_id=:epoch AND env=:env
              AND strategy_owner='US_STANDARD'
              AND revision_code=:revision AND active IS TRUE
              AND effective_at<=NOW()
              AND lifecycle_id=ANY(:lifecycles)
              AND account_id=ANY(:accounts)
            ORDER BY effective_at DESC
        """), {"epoch":epoch,"env":environment,"revision":REVISION_CODE,
               "lifecycles":sorted({k[1] for k in scopes}),
               "accounts":sorted({k[2] for k in scopes})}).mappings().all()
    results = {}
    for row in rows:
        item = dict(row)
        key = (str(item["symbol"]).upper(),str(item["lifecycle_id"]),str(item["account_id"]))
        if key in scopes and item.get("source_contract_sha256")==scopes[key] and key not in results:
            results[key] = {**item,"_db_verified":True}
    return results


def attach_approved_revisions(positions: list[dict], *, env: str | None = None) -> list[dict]:
    """Attach evidence without modifying signed buy-time contract or original dicts.

    If the new table is unavailable, do not manufacture an override.  Record
    the failure so the rollout cannot be falsely reported as completed.
    """
    if not positions:
        return positions
    try:
        approved = load_approved_revisions(positions, env=env)
    except Exception as exc:
        logger.error("[US_STOP_REVISION][LOAD_UNAVAILABLE] type=%s", type(exc).__name__)
        return positions
    if not approved:
        return positions
    output = []
    for item in positions:
        pos = dict(item)
        symbol, life, account = scope_for_position(pos)
        rev = approved.get((symbol,life,account))
        if rev and str(rev.get("account_id")) == account:
            pos["_verified_pb1_stop_revision"] = rev
        output.append(pos)
    return output
