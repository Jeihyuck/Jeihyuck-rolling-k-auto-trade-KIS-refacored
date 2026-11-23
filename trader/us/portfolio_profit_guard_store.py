"""Durable, signed US_STANDARD portfolio guard episode planning.

This repository component ONLY writes audited intent reservations. A native
SELL is illegal until the canonical order router separately verifies every
policy, action, original BUY cycle, live KIS balance and pre-HTTP fence.
It cannot submit or mark a reserved order filled.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from trader.us.portfolio_profit_guard import (
    canonical, plan_verified_guard_sell, triggered_stage,
    verify_portfolio_policy, _decimal,
)

logger = logging.getLogger(__name__)


class PortfolioGuardUnavailable(RuntimeError):
    pass


def _get_policy(conn, *, account_hash: str, epoch: str, portfolio: str, now: datetime) -> tuple[dict, str] | None:
    policy_rows = conn.execute(text("""
        SELECT policy_id,policy_version,authorization_sha256,authorization_json
        FROM public.us_portfolio_guard_policy
        WHERE env='practice' AND sleeve='US_STANDARD'
          AND account_hash=:account AND trading_epoch_id=:epoch
          AND portfolio_epoch_id=:portfolio
          AND effective_at<=:now AND expires_at>:now
        ORDER BY signed_at DESC
        LIMIT 2 FOR UPDATE
    """), {"account": account_hash, "epoch": epoch, "portfolio": portfolio, "now": now}).mappings().all()
    if not policy_rows:
        return None
    if len(policy_rows) != 1:
        raise PortfolioGuardUnavailable("PORT_GUARD_MULTIPLE_ACTIVE_MANIFESTS")
    row = dict(policy_rows[0])
    auth = row["authorization_json"]
    policy, digest = verify_portfolio_policy(
        auth, signing_key=os.getenv("US_PORTFOLIO_GUARD_SIGNING_KEY", ""),
        account_hash=account_hash, trading_epoch_id=epoch,
        portfolio_epoch_id=portfolio, now_utc=now,
    )
    if (digest != row["authorization_sha256"]
            or policy["policy_id"] != row["policy_id"]
            or policy["policy_version"] != row["policy_version"]):
        raise PortfolioGuardUnavailable("PORT_GUARD_DB_POLICY_DIGEST_CONFLICT")
    return policy, digest


def _action_key(
    *, policy: dict, account_hash: str, epoch: str, portfolio: str,
    episode_id: str, stage: int, cycle: str,
) -> str:
    material = {
        "purpose": "US_STANDARD_PORTFOLIO_GUARD_SELL_ACTION_V1",
        "policy_id": policy["policy_id"],
        "version": policy["policy_version"], "account": account_hash,
        "epoch": epoch, "portfolio": portfolio,
        "episode_id": episode_id, "stage": stage, "cycle": cycle,
    }
    return "US_PPG_" + hashlib.sha256(canonical(material)).hexdigest()[:32]


def persist_guard_observation_and_reserve(
    conn, *, account_hash: str, trading_epoch_id: str,
    portfolio_epoch_id: str, verified_pnl_usd: Decimal,
    verified_accounting: dict, complete_native_positions: list[dict],
    snapshot_id: str, snapshot_at: datetime,
    existing_sell_symbols: set[str],
) -> dict:
    """One PostgreSQL transaction holds policy/state/episode lock until commit.

    Caller MUST provide already reconciled KIS-full-balance plus native fill
    ledger proof, with no synthetic row. An operator-approved, signed policy
    and the schema are mandatory. At most one distinct episode/stage/cycle
    action is inserted. Reusing old stage doesn't re-send a broker order.
    """
    if not isinstance(snapshot_at, datetime) or snapshot_at.tzinfo is None:
        raise PortfolioGuardUnavailable("PORT_GUARD_SNAPSHOT_TIME_UNVERIFIED")
    if not snapshot_id or not isinstance(verified_accounting, dict):
        raise PortfolioGuardUnavailable("PORT_GUARD_ACCOUNTING_PROOF_MISSING")
    if verified_accounting.get("method") != "NATIVE_FILLS_PLUS_FRESH_KIS_MARK_V1":
        raise PortfolioGuardUnavailable("PORT_GUARD_ACCOUNTING_METHOD_MISMATCH")
    if any(p.get("snapshot_native_kis") is not True for p in complete_native_positions):
        raise PortfolioGuardUnavailable("PORT_GUARD_KIS_POSITION_PROOF_MISSING")
    pnl = _decimal(verified_pnl_usd)
    auth = _get_policy(
        conn, account_hash=account_hash, epoch=trading_epoch_id,
        portfolio=portfolio_epoch_id, now=snapshot_at,
    )
    if auth is None:
        return {"state": "UNAUTHORIZED", "intents": [], "reason": "NO_ACTIVE_SIGNED_POLICY"}
    policy, digest = auth
    scope = {
        "policy_id": policy["policy_id"], "policy_version": policy["policy_version"],
        "account": account_hash, "epoch": trading_epoch_id,
        "portfolio": portfolio_epoch_id,
    }
    state_rows = conn.execute(text("""
        SELECT high_water_pnl_usd,latest_pnl_usd,last_snapshot_at,
               episode_id,episode_anchor,stage_no,state,revision
        FROM public.us_portfolio_guard_state
        WHERE policy_id=:policy_id AND policy_version=:policy_version
          AND env='practice' AND account_hash=:account
          AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
        FOR UPDATE
    """), scope).mappings().all()
    if len(state_rows) > 1:
        raise PortfolioGuardUnavailable("PORT_GUARD_STATE_DUPLICATE")
    if not state_rows:
        # Mid-epoch activation must NOT backdate an unknown intraday high.
        # The first fully proven observation defines only the prospective
        # guard's HWM, after this signed policy's effective_at.
        conn.execute(text("""
            INSERT INTO public.us_portfolio_guard_state
            (policy_id,policy_version,env,account_hash,trading_epoch_id,
             portfolio_epoch_id,sleeve,accounting_method,baseline_provenance,
             high_water_pnl_usd,latest_pnl_usd,last_snapshot_id,last_snapshot_at,
             state,revision)
            VALUES (:policy_id,:policy_version,'practice',:account,:epoch,
                    :portfolio,'US_STANDARD',:method,
                    CAST(:baseline AS jsonb),:pnl,:pnl,:snapshot,:at,'WATCH',1)
            ON CONFLICT DO NOTHING
        """), {**scope, "method": verified_accounting["method"], "pnl": pnl,
               "snapshot": snapshot_id, "at": snapshot_at,
               "baseline": json.dumps({"snapshot_id": snapshot_id,
                                       "source": "FIRST_VERIFIED_AFTER_POLICY_EFFECTIVE",
                                       "pnl_usd": str(pnl)}, sort_keys=True)})
        state_rows = conn.execute(text("""
            SELECT high_water_pnl_usd,latest_pnl_usd,last_snapshot_at,
                   episode_id,episode_anchor,stage_no,state,revision
            FROM public.us_portfolio_guard_state
            WHERE policy_id=:policy_id AND policy_version=:policy_version
              AND env='practice' AND account_hash=:account
              AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
            FOR UPDATE
        """), scope).mappings().all()
        if len(state_rows) != 1:
            raise PortfolioGuardUnavailable("PORT_GUARD_BASELINE_LOCK_UNAVAILABLE")
        if str(state_rows[0]["last_snapshot_at"]) != str(snapshot_at):
            # Parallel worker installed a baseline earlier: do not suppress it
            # by overwriting with this snapshot. Retry next tick instead.
            return {"state": "BLOCKED", "intents": [], "reason": "BASELINE_RACE_RECHECK"}
        return {"state": "WATCH", "intents": [], "reason": "FIRST_VERIFIED_BASELINE"}
    prior = dict(state_rows[0])
    if prior["last_snapshot_at"] and prior["last_snapshot_at"] >= snapshot_at:
        return {"state": "BLOCKED", "intents": [], "reason": "STALE_OR_DUPLICATE_SNAPSHOT"}
    hwm = max(_decimal(prior["high_water_pnl_usd"]), pnl)
    old_stage = int(prior["stage_no"] or 0)
    stage = triggered_stage(pnl=pnl, hwm=hwm, policy=policy)
    if old_stage > stage:
        stage = old_stage  # never reset an active episode on market rebound
    conn.execute(text("""
        UPDATE public.us_portfolio_guard_state
        SET high_water_pnl_usd=:hwm,latest_pnl_usd=:pnl,
            last_snapshot_id=:snapshot,last_snapshot_at=:at,
            revision=revision+1,updated_at=now()
        WHERE policy_id=:policy_id AND policy_version=:policy_version
          AND env='practice' AND account_hash=:account
          AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
    """), {**scope, "hwm": hwm, "pnl": pnl,
           "snapshot": snapshot_id, "at": snapshot_at})
    if prior["state"] in ("SUBMIT_UNKNOWN", "BLOCKED"):
        return {"state": prior["state"], "intents": [], "reason": "UNRESOLVED_OR_BLOCKED_EPISODE"}
    if stage <= 0:
        return {"state": "WATCH", "intents": [], "hwm": str(hwm), "pnl": str(pnl)}
    current_episode = str(prior.get("episode_id") or "")
    if not current_episode:
        current_episode = hashlib.sha256(canonical({
            "policy": digest, "account": account_hash, "epoch": trading_epoch_id,
            "portfolio": portfolio_epoch_id, "first_trigger_snapshot": snapshot_id,
        })).hexdigest()[:32]
        reference_exposure = sum(
            _decimal(p["holding_qty"]) * _decimal(p["mark_bid_usd"])
            for p in complete_native_positions
        )
        episode_anchor = {
            "snapshot_id": snapshot_id,
            "reference_exposure_usd": str(reference_exposure),
            "initial_hwm_usd": str(hwm),
        }
        conn.execute(text("""
            UPDATE public.us_portfolio_guard_state
            SET episode_id=:episode,episode_anchor=CAST(:anchor AS jsonb),
                state='TRIGGERED',stage_no=:stage,updated_at=now()
            WHERE policy_id=:policy_id AND policy_version=:policy_version
              AND env='practice' AND account_hash=:account
              AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
        """), {**scope, "episode": current_episode,
               "anchor": json.dumps(episode_anchor, sort_keys=True), "stage": stage})
    else:
        episode_anchor = prior["episode_anchor"]
        if not isinstance(episode_anchor, dict):
            raise PortfolioGuardUnavailable("PORT_GUARD_EPISODE_ANCHOR_MISSING")
        reference_exposure = _decimal(episode_anchor["reference_exposure_usd"])
        if stage > old_stage:
            conn.execute(text("""
                UPDATE public.us_portfolio_guard_state
                SET stage_no=:stage,state='TRIGGERED',updated_at=now()
                WHERE policy_id=:policy_id AND policy_version=:policy_version
                  AND env='practice' AND account_hash=:account
                  AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
            """), {**scope, "stage": stage})
    existing_actions = conn.execute(text("""
        SELECT symbol,position_cycle_id,action_key,state,target_qty,
               cumulative_verified_fill_qty,decision_json
        FROM public.us_portfolio_guard_actions
        WHERE policy_id=:policy_id AND policy_version=:policy_version
          AND env='practice' AND account_hash=:account
          AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
          AND episode_id=:episode
        FOR UPDATE
    """), {**scope, "episode": current_episode}).mappings().all()
    if any(a["state"] not in ("COMPLETE",) for a in existing_actions):
        return {"state": "PARTIAL", "intents": [], "reason": "PREVIOUS_ACTION_NOT_TERMINAL"}
    reduced = sum(
        _decimal(a["cumulative_verified_fill_qty"])
        * _decimal((a["decision_json"] or {})["reference_price_usd"])
        for a in existing_actions
    )
    target_plans = plan_verified_guard_sell(
        complete_native_positions, policy=policy, stage_no=stage,
        reference_exposure_usd=reference_exposure, already_reduced_usd=reduced,
        excluded_symbols=existing_sell_symbols,
    )
    if not target_plans:
        return {"state": "PARTIAL", "intents": [],
                "reason": "NO_VERIFIED_ORDERABLE_CANDIDATE_OR_TARGET_COMPLETE"}
    inserted = []
    for plan in target_plans:
        action_key = _action_key(
            policy=policy, account_hash=account_hash, epoch=trading_epoch_id,
            portfolio=portfolio_epoch_id, episode_id=current_episode,
            stage=stage, cycle=plan["position_cycle_id"],
        )
        decision = {
            **plan, "snapshot_id": snapshot_id, "episode_id": current_episode,
            "stage_no": stage, "policy_sha256": digest,
            "policy_id": policy["policy_id"], "policy_version": policy["policy_version"],
        }
        result = conn.execute(text("""
            INSERT INTO public.us_portfolio_guard_actions
            (policy_id,policy_version,env,account_hash,trading_epoch_id,
             portfolio_epoch_id,episode_id,stage_no,symbol,position_cycle_id,
             action_key,target_qty,state,manifest_sha256,snapshot_id,decision_json)
            VALUES (:policy_id,:policy_version,'practice',:account,:epoch,
                    :portfolio,:episode,:stage,:symbol,:cycle,
                    :action_key,:qty,'PLANNED',:digest,:snapshot,
                    CAST(:decision AS jsonb))
            ON CONFLICT DO NOTHING
        """), {
            **scope, "episode": current_episode, "stage": stage,
            "symbol": plan["symbol"], "cycle": plan["position_cycle_id"],
            "action_key": action_key, "qty": plan["qty"], "digest": digest,
            "snapshot": snapshot_id, "decision": json.dumps(decision, sort_keys=True),
        })
        if result.rowcount == 1:
            inserted.append({**decision, "action_key": action_key})
    conn.execute(text("""
        UPDATE public.us_portfolio_guard_state SET state='PLANNED',updated_at=now()
        WHERE policy_id=:policy_id AND policy_version=:policy_version
          AND env='practice' AND account_hash=:account
          AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
    """), scope)
    return {"state": "PLANNED", "intents": inserted, "hwm": str(hwm),
            "pnl": str(pnl), "episode_id": current_episode}
