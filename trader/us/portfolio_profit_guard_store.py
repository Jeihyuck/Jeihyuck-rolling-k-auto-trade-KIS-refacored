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
    verify_portfolio_policy, _decimal, ACCOUNTING_METHODS, NET_ACCOUNTING_METHOD,
    TRADING_ACCOUNTING_METHOD, LEGACY_ACCOUNTING_SCOPE,
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


def reconcile_guard_native_order_actions(
    conn, *, policy: dict, account_hash: str, epoch: str, portfolio: str,
    episode: str,
) -> dict:
    """Reconcile guarded actions from original native-filled orders only.

    NEVER infer a CANCEL from the absence of an order, and never convert a
    nonauthoritative client ACK into a fill. For any unresolved/UNKNOWN state,
    the previously reserved stage remains fenced.
    """
    scope = {
        "policy_id": policy["policy_id"], "policy_version": policy["policy_version"],
        "account": account_hash, "epoch": epoch, "portfolio": portfolio,
        "episode": episode,
    }
    records = conn.execute(text("""
        SELECT a.action_key,a.symbol,a.position_cycle_id,a.target_qty,
               a.cumulative_verified_fill_qty,a.state,
               o.status AS order_status,o.qty_filled AS broker_qty_filled,
               o.order_no,o.meta AS order_meta,o.client_order_key,
               o.trading_epoch_id,o.portfolio_epoch_id,
               o.position_cycle_id AS order_cycle,
               o.account_id AS order_account
        FROM public.us_portfolio_guard_actions a
        LEFT JOIN public.us_orders o
          ON o.client_order_key=a.action_key
         AND o.side='SELL' AND o.env='practice'
         AND o.trading_epoch_id=a.trading_epoch_id
         AND o.portfolio_epoch_id=a.portfolio_epoch_id
         AND o.position_cycle_id=a.position_cycle_id
        WHERE a.policy_id=:policy_id AND a.policy_version=:policy_version
          AND a.env='practice' AND a.account_hash=:account
          AND a.trading_epoch_id=:epoch AND a.portfolio_epoch_id=:portfolio
          AND a.episode_id=:episode
        ORDER BY a.stage_no,a.symbol
        FOR UPDATE OF a
    """), scope).mappings().all()
    changes = 0
    for row in records:
        state = str(row["state"])
        if state == "COMPLETE":
            continue
        if row["order_no"] is None:
            # No KIS row is not proof the broker did not accept the order.
            # PLANNED is safe only if no canonical claim has yet been made,
            # checked by the router. Never retry a formerly unknown order.
            if state != "PLANNED":
                return {"state": "BLOCKED", "reason": "GUARD_ORDER_IDENTITY_UNVERIFIED"}
            continue
        meta = row.get("order_meta") if isinstance(row.get("order_meta"), dict) else {}
        if (
            str(row.get("order_account") or "") not in ("", account_hash)
            or str(row.get("order_cycle") or "") != row["position_cycle_id"]
            or str(row.get("trading_epoch_id") or "") != epoch
            or str(row.get("portfolio_epoch_id") or "") != portfolio
            or str(meta.get("portfolio_guard_action_key") or "") != row["action_key"]
            or str(row.get("client_order_key") or "") != row["action_key"]
        ):
            raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_ORDER_SCOPE_MISMATCH")
        status = str(row.get("order_status") or "").upper()
        try:
            observed_qty = int(row.get("broker_qty_filled") or 0)
            target = int(row["target_qty"])
        except (ValueError, TypeError):
            raise PortfolioGuardUnavailable("PORT_GUARD_ORDER_FILL_QTY_UNVERIFIED")
        if observed_qty < 0 or observed_qty > target:
            raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_ORDER_OVERFILLED")
        # Only native ledger fill rows, never synthetic local ACK, can satisfy.
        actual = conn.execute(text("""
            SELECT COALESCE(SUM(qty),0) AS qty
            FROM public.us_fills
            WHERE symbol=:symbol AND side='SELL'
              AND account_id=:account
              AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
              AND position_cycle_id=:cycle
              AND client_order_key=:key
              AND meta->>'fill_evidence_type'='KIS_ORDER_CUMULATIVE_ACTUAL'
              AND COALESCE((meta->>'is_synthetic')::boolean,false)=false
              AND COALESCE((meta->>'accounting_active')::boolean,true)=true
        """), {"symbol":row["symbol"],"epoch":epoch,
               "portfolio":portfolio,"cycle":row["position_cycle_id"],
               "account":account_hash,
               "key":row["action_key"]}).mappings().one()
        native_filled = int(actual["qty"] or 0)
        if native_filled != observed_qty:
            # Even FILLED broker row is not terminal before Stage4's native
            # cumulative actual persistence is durable.
            return {"state": "BLOCKED", "reason": "GUARD_NATIVE_FILL_RECON_PENDING"}
        if status in ("FILLED", "RECONCILED") and native_filled == target:
            transition = "COMPLETE"
        elif status in ("FILLED", "RECONCILED", "PARTIALLY_FILLED") and native_filled > 0:
            transition = "PARTIAL"
        elif status in ("OPEN", "ACK", "SUBMITTED", "ACCEPTED") and native_filled == 0:
            transition = "SUBMITTED"
        else:
            transition = "SUBMIT_UNKNOWN"
        # Never downgrade a previously terminal stage, or erase uncertainty.
        if state == "SUBMIT_UNKNOWN" and transition != "COMPLETE":
            return {"state": "BLOCKED", "reason": "GUARD_SUBMIT_UNKNOWN_REQUIRES_BROKER_PROOF"}
        conn.execute(text("""
            UPDATE public.us_portfolio_guard_actions
            SET state=:next,cumulative_verified_fill_qty=:qty,updated_at=now()
            WHERE action_key=:key AND state=:previous
        """), {"next":transition,"qty":native_filled,
               "key":row["action_key"],"previous":state})
        changes += 1
    return {"state": "OK", "updated": changes}


def proven_episode_sells_at_frozen_prices(
    episode_anchor: dict, native_sells_by_cycle: dict[str, int],
) -> Decimal:
    """Count only actual KIS-filled shares after trigger at ORIGINAL prices.

    TP/stop and portfolio SELLs reduce the same cumulative exposure target.
    Mark-to-market price drops and unconfirmed attempts NEVER count.
    """
    original_lots = episode_anchor.get("original_lots")
    if not isinstance(original_lots, dict) or not original_lots:
        raise PortfolioGuardUnavailable("PORT_GUARD_ORIGINAL_LOTS_PROOF_MISSING")
    reduced = Decimal("0")
    for cycle, original in original_lots.items():
        before = int(original["initial_native_sell_qty"])
        actual = native_sells_by_cycle.get(cycle, 0)
        if isinstance(actual, bool) or not isinstance(actual, int) or actual < before:
            raise PortfolioGuardUnavailable("PORT_GUARD_SELL_LEDGER_REGRESSED")
        sold_since = actual - before
        if sold_since > int(original["initial_holding_qty"]):
            raise PortfolioGuardUnavailable("PORT_GUARD_SELL_MORE_THAN_INITIAL_HOLDINGS")
        reduced += Decimal(sold_since) * _decimal(original["reference_price_usd"])
    cap = _decimal(episode_anchor["reference_exposure_usd"])
    if reduced > cap:
        raise PortfolioGuardUnavailable("PORT_GUARD_REDUCED_VALUE_OVERFLOW")
    return reduced


def persist_guard_observation_and_reserve(
    conn, *, account_hash: str, trading_epoch_id: str,
    portfolio_epoch_id: str, verified_pnl_usd: Decimal,
    verified_accounting: dict, complete_native_positions: list[dict],
    snapshot_id: str, snapshot_at: datetime,
    existing_sell_symbols: set[str],
    native_verified_sold_qty_by_cycle: dict[str, int] | None = None,
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
    if verified_accounting.get("method") not in ACCOUNTING_METHODS:
        raise PortfolioGuardUnavailable("PORT_GUARD_ACCOUNTING_METHOD_MISMATCH")
    if any(p.get("snapshot_native_kis") is not True for p in complete_native_positions):
        raise PortfolioGuardUnavailable("PORT_GUARD_KIS_POSITION_PROOF_MISSING")
    pnl = _decimal(verified_pnl_usd)
    if native_verified_sold_qty_by_cycle is None:
        raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_SELL_LEDGER_REQUIRED")
    if any(
        not cycle or isinstance(qty, bool) or not isinstance(qty, int) or qty < 0
        for cycle, qty in native_verified_sold_qty_by_cycle.items()
    ):
        raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_SELL_LEDGER_INVALID")
    auth = _get_policy(
        conn, account_hash=account_hash, epoch=trading_epoch_id,
        portfolio=portfolio_epoch_id, now=snapshot_at,
    )
    if auth is None:
        return {"state": "UNAUTHORIZED", "intents": [], "reason": "NO_ACTIVE_SIGNED_POLICY"}
    policy, digest = auth
    if verified_accounting["method"] != policy["accounting_method"]:
        raise PortfolioGuardUnavailable("PORT_GUARD_SIGNED_ACCOUNTING_METHOD_MISMATCH")
    if verified_accounting.get("legacy_fill_count", 0) and (
        policy.get("legacy_accounting_scope") != LEGACY_ACCOUNTING_SCOPE
        or verified_accounting.get("legacy_accounting_scope") != LEGACY_ACCOUNTING_SCOPE
    ):
        raise PortfolioGuardUnavailable("PORT_GUARD_SIGNED_LEGACY_SCOPE_MISMATCH")
    scope = {
        "policy_id": policy["policy_id"], "policy_version": policy["policy_version"],
        "account": account_hash, "epoch": trading_epoch_id,
        "portfolio": portfolio_epoch_id,
    }
    state_rows = conn.execute(text("""
        SELECT high_water_pnl_usd,latest_pnl_usd,last_snapshot_at,
               accounting_method,baseline_provenance,
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
                                       "pnl_usd": str(pnl),
                                       "policy_sha256": digest}, sort_keys=True)})
        state_rows = conn.execute(text("""
            SELECT high_water_pnl_usd,latest_pnl_usd,last_snapshot_at,
               accounting_method,baseline_provenance,
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
    if prior["accounting_method"] != policy["accounting_method"]:
        raise PortfolioGuardUnavailable("PORT_GUARD_HWM_ACCOUNTING_METHOD_MISMATCH")
    baseline = prior.get("baseline_provenance") or {}
    if baseline.get("policy_sha256") not in (None, digest):
        raise PortfolioGuardUnavailable("PORT_GUARD_HWM_POLICY_DIGEST_MISMATCH")
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
        original_lots = {
            str(p["position_cycle_id"]): {
                "symbol": str(p["symbol"]).upper(),
                "reference_price_usd": str(_decimal(p["mark_bid_usd"])),
                "initial_holding_qty": int(p["holding_qty"]),
                "initial_native_sell_qty": int(native_verified_sold_qty_by_cycle.get(
                    str(p["position_cycle_id"]), 0,
                )),
            }
            for p in complete_native_positions
            if str(p.get("position_cycle_id") or "")
        }
        if len(original_lots) != len(complete_native_positions):
            raise PortfolioGuardUnavailable("PORT_GUARD_REFERENCE_CYCLE_MISSING_OR_DUPLICATE")
        episode_anchor = {
            "snapshot_id": snapshot_id,
            "triggered_at_utc": snapshot_at.isoformat(),
            "reference_exposure_usd": str(reference_exposure),
            "initial_hwm_usd": str(hwm),
            "triggered_pnl_usd": str(pnl),
            "original_lots": original_lots,
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
    observed_actions = reconcile_guard_native_order_actions(
        conn, policy=policy, account_hash=account_hash,
        epoch=trading_epoch_id, portfolio=portfolio_epoch_id,
        episode=current_episode,
    )
    if observed_actions.get("state") != "OK":
        return {"state": "BLOCKED", "intents": [], "reason": observed_actions["reason"]}
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
    # Include ALL independently proven post-trigger native SELLs, including
    # the ordinary hard/soft stop and TP fills. No double accounting for
    # portfolio guard executions. Every share is valued at frozen trigger bid.
    original_lots = episode_anchor.get("original_lots")
    if not isinstance(original_lots, dict):
        raise PortfolioGuardUnavailable("PORT_GUARD_ORIGINAL_LOTS_PROOF_MISSING")
    reduced = proven_episode_sells_at_frozen_prices(
        episode_anchor, native_verified_sold_qty_by_cycle,
    )
    target_plans = plan_verified_guard_sell(
        complete_native_positions, policy=policy, stage_no=stage,
        reference_exposure_usd=reference_exposure, already_reduced_usd=reduced,
        excluded_symbols=existing_sell_symbols,
        episode_reference_prices={
            cycle: original["reference_price_usd"]
            for cycle, original in original_lots.items()
        },
    )
    remaining_dollars = max(
        Decimal("0"),
        reference_exposure
        * _decimal(policy["stages"][stage - 1]["cumulative_reduction_fraction"])
        - reduced,
    )
    if not target_plans:
        if remaining_dollars < _decimal(policy["min_order_notional_usd"]):
            # A signed minimum executable lot defines the only allowed dust
            # tolerance. Never mistake an unfilled pending order for progress.
            conn.execute(text("""
                UPDATE public.us_portfolio_guard_state
                SET state='COMPLETE',updated_at=now()
                WHERE policy_id=:policy_id AND policy_version=:policy_version
                  AND env='practice' AND account_hash=:account
                  AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
            """), scope)
            return {"state": "COMPLETE", "intents": [],
                    "reason": "NATIVE_SELL_TARGET_MET_WITH_SIGNED_DUST_TOLERANCE"}
        return {"state": "PARTIAL", "intents": [],
                "reason": "TARGET_NOT_MET_OR_NO_VERIFIED_ORDERABLE_CANDIDATE"}
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
    if not inserted:
        # Existing episode/stage/cycle action can never be silently
        # overwritten to issue another native order after restart.
        # It may be COMPLETE, but this stage's fixed exposure target is
        # not yet independently proven satisfied: operator review.
        return {
            "state": "BLOCKED", "intents": [],
            "reason": "GUARD_ACTION_IDENTITY_ALREADY_RESERVED",
        }
    conn.execute(text("""
        UPDATE public.us_portfolio_guard_state SET state='PLANNED',updated_at=now()
        WHERE policy_id=:policy_id AND policy_version=:policy_version
          AND env='practice' AND account_hash=:account
          AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
    """), scope)
    return {"state": "PLANNED", "intents": inserted, "hwm": str(hwm),
            "pnl": str(pnl), "episode_id": current_episode}


def assert_portfolio_guard_native_pre_http(
    intent: dict, *, now: datetime, actual_qty: int,
    actual_limit_price: Decimal, kis_client: Any = None,
) -> None:
    """Independent signed-policy + durable-stage check at KIS POST boundary.

    No bearer authority is inferred from exit_type or an intent-only hash.
    This check must run AFTER the generic canonical router fences, and the
    broker order is still subject to fresh KIS holdings/price and Stage4.
    """
    if os.getenv("US_PORTFOLIO_GUARD_ACTIVE", "0") != "1":
        raise PortfolioGuardUnavailable("PORT_GUARD_NOT_OPERATOR_ACTIVE")
    if (
        os.getenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "0") != "1"
        or os.getenv("US_ACCOUNT_RESERVATION_MODE", "") != "ENFORCE"
    ):
        raise PortfolioGuardUnavailable("PORT_GUARD_SHARED_RESERVATION_REQUIRED")
    if kis_client is None or not callable(getattr(kis_client, "get_us_balance", None)):
        raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_KIS_BALANCE_UNAVAILABLE")
    if os.getenv("KIS_ENV", "").lower() != "practice":
        raise PortfolioGuardUnavailable("PORT_GUARD_PRACTICE_ONLY")
    if now.tzinfo is None:
        raise PortfolioGuardUnavailable("PORT_GUARD_HTTP_TIME_UNVERIFIED")
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    sym = str(intent.get("symbol") or "").upper()
    if (
        intent.get("side") != "SELL"
        or intent.get("exit_type") != "portfolio_profit_guard"
        or intent.get("strategy_owner") != "US_STANDARD"
        or sym == "TQQQ"
        or not sym
        or actual_qty <= 0
        or _decimal(actual_limit_price) <= 0
    ):
        raise PortfolioGuardUnavailable("PORT_GUARD_ORDER_OWNER_OR_AMOUNT_INVALID")
    expected_key = str(meta.get("portfolio_guard_action_key") or "")
    if not expected_key.startswith("US_PPG_"):
        raise PortfolioGuardUnavailable("PORT_GUARD_ACTION_KEY_MISSING")
    if str(intent.get("client_order_key") or "") != expected_key:
        raise PortfolioGuardUnavailable("PORT_GUARD_ORIGINAL_ACTION_IDENTITY_MISMATCH")
    # A genuine, fresh KIS balance must still authorize every share after
    # the ordinary stop/TP claims have been made; the planning snapshot is
    # NOT a broker orderable authority at POST.
    from trader.us.data_provider import normalize_us_balance
    raw_balance = kis_client.get_us_balance(force_refresh=True)
    if (
        not isinstance(raw_balance, dict)
        or raw_balance.get("balance_authoritative") is not True
        or raw_balance.get("balance_complete") is not True
        or raw_balance.get("failed_exchanges")
    ):
        raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_BALANCE_INCOMPLETE")
    normalized_balance = normalize_us_balance(raw_balance)
    if normalized_balance.get("balance_parse_status") != "OK":
        raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_BALANCE_PARSE_FAILED")
    native_positions = [
        p for p in normalized_balance.get("positions", [])
        if str(p.get("symbol") or "").upper() == sym
    ]
    if len(native_positions) != 1:
        raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_HOLDING_AMBIGUOUS")
    if int(native_positions[0].get("orderable_qty") or 0) < actual_qty:
        raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_ORDERABLE_EXCEEDED")
    from trader.account_state import get_account_key
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
    from trader.us.execution.recovery_identity import current_us_portfolio_epoch
    raw_account = get_account_key(env="practice")
    if not raw_account:
        raise PortfolioGuardUnavailable("PORT_GUARD_ACCOUNT_UNVERIFIED")
    account_hash = hashlib.sha256(raw_account.encode()).hexdigest()
    engine = _get_engine_or_none()
    if engine is None:
        raise PortfolioGuardUnavailable("PORT_GUARD_DATABASE_UNAVAILABLE")
    with engine.begin() as conn:
        epoch = str(_active_us_epoch(conn, required=True))
        portfolio = current_us_portfolio_epoch(
            conn, env="practice", account_key=raw_account,
            trading_epoch_id=epoch, strategy_owner="US_STANDARD",
        )
        manifest = _get_policy(
            conn, account_hash=account_hash, epoch=epoch,
            portfolio=portfolio, now=now,
        )
        if manifest is None:
            raise PortfolioGuardUnavailable("PORT_GUARD_POLICY_NOT_ACTIVE")
        policy, digest = manifest
        if (
            str(meta.get("portfolio_guard_policy_digest") or "") != digest
            or str(meta.get("portfolio_guard_policy_id") or "") != policy["policy_id"]
            or str(meta.get("portfolio_guard_policy_version") or "") != policy["policy_version"]
            or str(meta.get("portfolio_guard_portfolio_epoch_id") or "") != portfolio
            or str(meta.get("portfolio_guard_trading_epoch_id") or "") != epoch
        ):
            raise PortfolioGuardUnavailable("PORT_GUARD_POLICY_INTENT_MISMATCH")
        try:
            source_quote_at = datetime.fromisoformat(
                str(meta.get("portfolio_guard_quote_asof_utc") or "").replace("Z", "+00:00")
            )
        except (TypeError, ValueError) as exc:
            raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_QUOTE_TIME_MISSING") from exc
        if source_quote_at.tzinfo is None:
            raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_QUOTE_TIME_NAIVE")
        age = Decimal(str((now - source_quote_at).total_seconds()))
        if not (0 <= age <= _decimal(policy["quote_ttl_sec"])):
            raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_QUOTE_EXPIRED")
        mark_bid = _decimal(meta.get("portfolio_guard_verified_bid_usd"))
        if mark_bid <= 0 or _decimal(actual_limit_price) > mark_bid:
            raise PortfolioGuardUnavailable("PORT_GUARD_LIMIT_ABOVE_NATIVE_BID")
        rows = conn.execute(text("""
            SELECT symbol,position_cycle_id,stage_no,episode_id,
                   target_qty,cumulative_verified_fill_qty,reserved_qty,
                   state,manifest_sha256
            FROM public.us_portfolio_guard_actions
            WHERE action_key=:action
              AND policy_id=:policy AND policy_version=:version
              AND env='practice' AND account_hash=:account
              AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
            FOR UPDATE
        """), {
            "action": expected_key, "policy": policy["policy_id"],
            "version": policy["policy_version"], "account": account_hash,
            "epoch": epoch, "portfolio": portfolio,
        }).mappings().all()
        if len(rows) != 1:
            raise PortfolioGuardUnavailable("PORT_GUARD_DURABLE_ACTION_MISSING")
        action = dict(rows[0])
        if (
            action["manifest_sha256"] != digest
            or action["symbol"] != sym
            or action["position_cycle_id"] != str(intent.get("position_cycle_id") or "")
            or action["stage_no"] != int(meta.get("portfolio_guard_stage_no") or 0)
            or action["episode_id"] != str(meta.get("portfolio_guard_episode_id") or "")
            or action["state"] not in ("PLANNED",)
            or int(action["target_qty"]) - int(action["cumulative_verified_fill_qty"]) != actual_qty
            or int(action["reserved_qty"]) > 0
        ):
            raise PortfolioGuardUnavailable("PORT_GUARD_DURABLE_STAGE_OR_QTY_FENCED")
    logger.info(
        "[US_PORT_GUARD][PRE_HTTP_AUTHORIZED] symbol=%s action=%s stage=%s",
        sym, expected_key, meta.get("portfolio_guard_stage_no"),
    )


def build_authorized_portfolio_guard_intents(
    *, current_positions: list[dict], trade_date: str,
    env: str, authoritative_native_balance: bool,
    excluded_sell_symbols: set[str],
) -> list[dict]:
    """Eligible only AFTER normal stop/TP SELLs have been routed.

    Fetch owner-scoped native-fill economics and complete fresh KIS quote
    evidence, then commit HWM plus stable intent reservation atomically.
    Authorization remains independent of original signed BUY provenance.
    Returns intents for the EXISTING canonical router, never HTTP itself.
    Any invalid observation silently returns no new portfolio intent while
    the pre-existing individual risk exits remain uninterrupted.
    """
    if (
        os.getenv("US_PORTFOLIO_GUARD_ACTIVE", "0") != "1"
        or env != "practice"
        or os.getenv("KIS_ENV", "").lower() != "practice"
        or not authoritative_native_balance
        or os.getenv("US_WS_COLLECTOR_ENABLED", "0") != "1"
        or os.getenv("US_SHARED_KIS_LIMITER_ENABLED", "0") != "1"
    ):
        return []
    from trader.account_state import get_account_key
    from trader.us.db.repos import _get_engine_or_none, _active_us_epoch
    from trader.us.execution.recovery_identity import current_us_portfolio_epoch
    from trader.us.portfolio_profit_guard import verified_sleeve_pnl, _utc
    from trader.us.portfolio_guard_original_ledger import (
        fill_key, legacy_economic_row, load_legacy_epoch_proof,
        prepare_legacy_management_position,
    )
    from trader.us.strategy_ownership import owner_for_symbol, STANDARD_OWNER
    from trader.us.symbols import resolve_quote_exchange
    from trader.marketdata.us_ws_collector import get_quote
    from decimal import ROUND_FLOOR
    from datetime import timedelta
    raw_account = get_account_key(env="practice")
    if not raw_account:
        return []
    account_hash = hashlib.sha256(raw_account.encode()).hexdigest()
    engine = _get_engine_or_none()
    if engine is None:
        return []
    try:
        now = datetime.now(timezone.utc)
        with engine.begin() as conn:
            epoch = str(_active_us_epoch(conn, required=True))
            portfolio = current_us_portfolio_epoch(
                conn, env="practice", account_key=raw_account,
                trading_epoch_id=epoch, strategy_owner="US_STANDARD",
            )
            approved = _get_policy(
                conn, account_hash=account_hash, epoch=epoch,
                portfolio=portfolio, now=now,
            )
            if approved is None:
                # This mode is an executable order feature. A missing signed
                # manifest is NOT SHADOW, not successful ACTIVE, and never
                # authority to submit a discretionary portfolio SELL.
                raise PortfolioGuardUnavailable("GUARD_STARTUP_NO_ACTIVE_SIGNED_POLICY")
            policy, manifest_digest = approved
            # Per-symbol broker/order claim conflict fence across session and
            # prior trade dates.  A pending/unknown economic SELL cannot be
            # treated as a new portfolio-guard tranche just because the
            # other strategy's client key differs.
            native_pending = conn.execute(text("""
                SELECT DISTINCT symbol FROM public.us_orders
                WHERE env='practice' AND trading_epoch_id=:epoch
                  AND side='SELL'
                  AND COALESCE(meta->>'strategy_owner','US_STANDARD')='US_STANDARD'
                  AND status NOT IN ('FILLED','RECONCILED',
                                     'CANCELLED','CANCELED','REJECTED','EXPIRED')
            """), {"epoch": epoch}).mappings().all()
            unresolved_claim_cycles = conn.execute(text("""
                SELECT DISTINCT position_cycle_id FROM public.us_execution_claims
                WHERE env='practice' AND market='US'
                  AND account_id=:account AND trading_epoch_id=:epoch
                  AND strategy_owner='US_STANDARD'
                  AND action_state IN ('OPEN','IN_FLIGHT','UNCERTAIN',
                                       'PARTIALLY_SATISFIED','RETRYABLE')
            """), {"account": account_hash, "epoch": epoch}).mappings().all()
            blocked_cycles = {
                str(v["position_cycle_id"]) for v in unresolved_claim_cycles
                if v.get("position_cycle_id")
            }
            pending_standard_symbols = {
                str(v["symbol"]).upper() for v in native_pending if v.get("symbol")
            }
            native_fills = conn.execute(text("""
                SELECT trade_date,symbol,side,qty,price_usd,order_no,
                       client_order_key,account_id,trading_epoch_id,
                       portfolio_epoch_id,position_cycle_id,meta
                FROM public.us_fills
                WHERE trading_epoch_id=:epoch AND trade_date<=:day
                  AND COALESCE(meta->>'accounting_active','true') <> 'false'
                ORDER BY trade_date,id
            """), {"epoch": epoch, "day": trade_date}).mappings().all()
            native_fills = [
                dict(row) for row in native_fills
                if owner_for_symbol(str(row.get("symbol") or "")) == STANDARD_OWNER
            ]
            legacy_groups, legacy_fills = {}, {}
            has_legacy = any(legacy_economic_row(row) for row in native_fills)
            if has_legacy:
                if (policy["accounting_method"] != TRADING_ACCOUNTING_METHOD
                        or policy.get("legacy_accounting_scope") != LEGACY_ACCOUNTING_SCOPE):
                    raise PortfolioGuardUnavailable("PORT_GUARD_LEGACY_HISTORY_NOT_SIGNED")
                legacy_fills, legacy_groups = load_legacy_epoch_proof(
                    conn, native_fills, raw_account=raw_account, epoch=epoch,
                    portfolio=portfolio, trade_date=trade_date,
                )
            # Valuation includes every proven original economic group, even
            # closed lots. A live legacy lot obtains only an ephemeral forward
            # scope; the native router re-proves it before submitting.
            prepared_positions = []
            for position in current_positions:
                sym = str(position.get("symbol") or "").upper()
                pm = position.get("meta") if isinstance(position.get("meta"), dict) else {}
                if (sym in legacy_groups
                        and _decimal(position.get("qty") or position.get("holding_qty") or 0) > 0
                        and not (position.get("portfolio_epoch_id") or pm.get("portfolio_epoch_id"))):
                    position, unresolved = prepare_legacy_management_position(
                        conn, position, legacy_groups[sym], raw_account=raw_account,
                        epoch=epoch, portfolio=portfolio, trade_date=trade_date,
                    )
                    if unresolved:
                        pending_standard_symbols.add(sym)
                prepared_positions.append(position)
        fill_proof = []
        for fill in native_fills:
            meta = fill["meta"] if isinstance(fill["meta"], dict) else {}
            original_group = legacy_fills.get(fill_key(fill))
            if legacy_economic_row(fill):
                if not original_group:
                    raise PortfolioGuardUnavailable("PORT_GUARD_LEGACY_FILL_PROOF_MISSING")
                economic_cycle = original_group["management_cycle"]
            else:
                if (meta.get("strategy_owner") != "US_STANDARD"
                        or str(fill.get("portfolio_epoch_id") or "") != portfolio
                        or not fill.get("position_cycle_id")):
                    raise PortfolioGuardUnavailable("PORT_GUARD_FILL_OWNER_PORTFOLIO_CYCLE_UNVERIFIED")
                if str(fill.get("account_id") or "") not in (raw_account, account_hash):
                    raise PortfolioGuardUnavailable("PORT_GUARD_FILL_ACCOUNT_MISMATCH")
                economic_cycle = str(fill["position_cycle_id"])
            fill_proof.append({
                "symbol": str(fill["symbol"]).upper(),
                "side": fill["side"], "qty": fill["qty"],
                "price_usd": fill["price_usd"], "order_no": fill["order_no"],
                "client_order_key": fill["client_order_key"],
                "trade_date": str(fill["trade_date"]),
                "strategy_owner": meta.get("strategy_owner"),
                "account_hash": account_hash, "trading_epoch_id": epoch,
                "fill_evidence_type": meta.get("fill_evidence_type"),
                "is_synthetic": meta.get("is_synthetic") or meta.get("synthetic"),
                # The legacy cycle here is for in-memory economic grouping,
                # never a retroactive original BUY identity in the database.
                "position_cycle_id": economic_cycle,
                "original_runtime_lifecycle_id": original_group["original_cycle"] if original_group else None,
                # Only broker-proven native charge records may contribute
                # to net P&L; legacy fill metadata missing these fields
                # cannot be silently assumed to have zero commission.
                "commission_usd": meta.get("kis_verified_commission_usd"),
                "tax_usd": meta.get("kis_verified_tax_usd"),
                "native_fee_proof": meta.get("kis_native_fee_proof") is True,
            })
        # Original owner BUY/SELL cycle evidence, not order-intent guesses.
        sell_qty_by_cycle: dict[str, int] = {}
        net_qty_by_cycle: dict[str, int] = {}
        symbols_by_cycle: dict[str, str] = {}
        for proof in fill_proof:
            if proof["fill_evidence_type"] != "KIS_ORDER_CUMULATIVE_ACTUAL":
                raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_FILL_NOT_PROVEN")
            cycle = proof["position_cycle_id"]
            qty = _decimal(proof["qty"])
            if (qty <= 0 or qty != qty.to_integral_value()
                    or proof["side"] not in ("BUY", "SELL")
                    or symbols_by_cycle.setdefault(cycle, proof["symbol"]) != proof["symbol"]):
                raise PortfolioGuardUnavailable("PORT_GUARD_ECONOMIC_CYCLE_FILL_INVALID")
            net_qty_by_cycle[cycle] = net_qty_by_cycle.get(cycle, 0) + (
                int(qty) if proof["side"] == "BUY" else -int(qty)
            )
            if proof["side"] == "SELL":
                sell_qty_by_cycle[cycle] = sell_qty_by_cycle.get(cycle, 0) + int(qty)
        if any(qty < 0 for qty in net_qty_by_cycle.values()):
            raise PortfolioGuardUnavailable("PORT_GUARD_ECONOMIC_CYCLE_NEGATIVE_NET")
        # Profit-ranking cost basis is derived from verified original
        # native BUY fills, never a mutable position or provider average.
        # This changes no original BUY and never invents a new cost basis.
        native_buy_totals: dict[str, tuple[Decimal, Decimal]] = {}
        for proof in fill_proof:
            if proof["side"] != "BUY":
                continue
            # Multiple lifecycles of the same ticker cannot donate cost basis
            # to an unrelated current original BUY.
            cycle_key = str(proof["position_cycle_id"])
            qty_filled = _decimal(proof["qty"])
            price_filled = _decimal(proof["price_usd"])
            old_qty, old_cost = native_buy_totals.get(
                cycle_key, (Decimal("0"), Decimal("0")),
            )
            native_buy_totals[cycle_key] = (
                old_qty + qty_filled,
                old_cost + qty_filled * price_filled,
            )
        enriched_by_symbol: dict[str, dict] = {}
        native_valuations = []
        all_position_symbols = set()
        seen_open_cycles = set()
        for position in prepared_positions:
            sym = str(position.get("symbol") or "").upper()
            if not sym or sym in all_position_symbols:
                raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_POSITION_DUPLICATE")
            all_position_symbols.add(sym)
            registered_owner = owner_for_symbol(sym)
            if registered_owner != STANDARD_OWNER:
                continue  # TQQQ Infinite has its own sleeve
            meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
            if str(position.get("strategy_owner") or meta.get("strategy_owner") or "") != "US_STANDARD":
                raise PortfolioGuardUnavailable("PORT_GUARD_UNKNOWN_BUY_OWNER")
            raw_qty = int(position.get("qty") or position.get("holding_qty") or 0)
            if raw_qty <= 0:
                continue
            # Prove original signed BUY lineage and current owner epoch
            # BEFORE recording an irreversible stage reservation. Otherwise
            # an absent contract would strand a PLANNED action with zero
            # broker POSTs, blocking the sleeve indefinitely.
            original_cycle = str(
                position.get("position_cycle_id")
                or meta.get("position_cycle_id") or ""
            )
            source_sha = str(meta.get("entry_exit_contract_sha256") or "")
            source_version = str(meta.get("entry_exit_contract_version") or "")
            original_epoch = str(
                position.get("portfolio_epoch_id")
                or meta.get("portfolio_epoch_id") or ""
            )
            if (
                not original_cycle or not source_sha or not source_version
                or original_epoch != portfolio
            ):
                raise PortfolioGuardUnavailable(
                    "PORT_GUARD_ORIGINAL_BUY_CONTRACT_SCOPE_UNVERIFIED"
                )
            quote = get_quote(
                sym, resolve_quote_exchange(sym), env="practice",
                max_age_sec=float(policy["quote_ttl_sec"]), timeout_sec=0.12,
            )
            if not isinstance(quote, dict) or quote.get("bid") in (None, "", 0, "0"):
                raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_BID_UNAVAILABLE")
            bid = _decimal(quote["bid"])
            raw_ts = quote.get("received_at")
            if isinstance(raw_ts, (int, float)) and raw_ts > 0:
                source_ts = datetime.fromtimestamp(raw_ts, timezone.utc)
            elif isinstance(raw_ts, str) and raw_ts.replace(".", "", 1).isdigit():
                source_ts = datetime.fromtimestamp(float(raw_ts), timezone.utc)
            else:
                source_ts = _utc(raw_ts)
            pos_cycle = str(position.get("position_cycle_id") or meta.get("position_cycle_id") or "")
            if (pos_cycle in seen_open_cycles or net_qty_by_cycle.get(pos_cycle) != raw_qty
                    or symbols_by_cycle.get(pos_cycle) != sym):
                raise PortfolioGuardUnavailable("PORT_GUARD_CURRENT_CYCLE_NET_MISMATCH")
            seen_open_cycles.add(pos_cycle)
            orderable_qty = position.get("orderable_qty")
            if orderable_qty is None:
                raise PortfolioGuardUnavailable("PORT_GUARD_KIS_ORDERABLE_MISSING")
            native_buy_qty, native_buy_cost = native_buy_totals.get(
                pos_cycle, (Decimal("0"), Decimal("0")),
            )
            if native_buy_qty <= 0 or native_buy_cost <= 0:
                raise PortfolioGuardUnavailable("PORT_GUARD_NATIVE_BUY_COST_MISSING")
            latest = {
                "symbol": sym, "strategy_owner": "US_STANDARD",
                "account_hash": account_hash, "trading_epoch_id": epoch,
                "holding_qty": raw_qty, "orderable_qty": int(orderable_qty),
                "mark_bid_usd": str(bid), "mark_source": "KIS_BID",
                "snapshot_native_kis": True, "quote_asof_utc": source_ts.isoformat(),
                "position_cycle_id": pos_cycle,
                "unresolved_sell": (
                    sym in pending_standard_symbols or pos_cycle in blocked_cycles
                ),
                "verified_entry_usd": str(native_buy_cost / native_buy_qty),
            }
            native_valuations.append(latest)
            enriched_by_symbol[sym] = position
        if any(qty > 0 and cycle not in seen_open_cycles for cycle, qty in net_qty_by_cycle.items()):
            raise PortfolioGuardUnavailable("PORT_GUARD_UNACCOUNTED_OPEN_ECONOMIC_CYCLE")
        snapshot_at = datetime.now(timezone.utc)
        pnl, economics = verified_sleeve_pnl(
            fill_proof, native_valuations,
            snapshot_at=snapshot_at, account_hash=account_hash,
            trading_epoch_id=epoch,
            quote_ttl_sec=_decimal(policy["quote_ttl_sec"]),
            max_skew_sec=_decimal(policy["max_snapshot_skew_sec"]),
            broker_full_balance_verified=True,
            # A separately signed trading-PnL policy may use verified
            # pre-cost economics. The old net policy STILL requires native
            # fee and cash completeness; never silently fall back.
            accounting_method=policy["accounting_method"],
            broker_cash_ledger_complete=False,
            cash_events=None,
        )
        economics["legacy_fill_count"] = len(legacy_fills)
        economics["legacy_accounting_scope"] = LEGACY_ACCOUNTING_SCOPE if legacy_fills else None
        logger.info(
            "[US_PORT_GUARD][VALUATION] method=%s pnl_usd=%s confirmed_net_pnl_usd=%s fee_fill_count=%s",
            economics["method"], pnl, economics.get("confirmed_net_pnl_usd"),
            economics.get("verified_fee_fill_count"),
        )
        snapshot_id = hashlib.sha256(canonical({
            "accounting_method": economics["method"],
            "policy_sha256": manifest_digest, "epoch": epoch,
            "portfolio": portfolio, "quote_asof": [
                [p["symbol"], p["quote_asof_utc"], p["holding_qty"], p["mark_bid_usd"]]
                for p in sorted(native_valuations, key=lambda p: p["symbol"])
            ],
            "native_fill_count": len(fill_proof), "pnl_usd": str(pnl),
            "legacy_originals": [[sym, g["original_cycle"], g["management_cycle"],
                                  g["contract_sha"], g["net_qty"]]
                                 for sym, g in sorted(legacy_groups.items())],
        })).hexdigest()
        with engine.begin() as conn:
            stage_plan = persist_guard_observation_and_reserve(
                conn, account_hash=account_hash,
                trading_epoch_id=epoch, portfolio_epoch_id=portfolio,
                verified_pnl_usd=pnl, verified_accounting=economics,
                complete_native_positions=native_valuations,
                snapshot_id=snapshot_id, snapshot_at=snapshot_at,
                existing_sell_symbols={
                    str(symbol).upper() for symbol in excluded_sell_symbols
                } | pending_standard_symbols,
                native_verified_sold_qty_by_cycle=sell_qty_by_cycle,
            )
        if stage_plan.get("state") != "PLANNED":
            return []
        output = []
        for p in stage_plan["intents"]:
            position = enriched_by_symbol[p["symbol"]]
            meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
            cycle = str(p["position_cycle_id"])
            if (not cycle
                    or cycle != str(position.get("position_cycle_id") or meta.get("position_cycle_id") or "")
                    or str(position.get("portfolio_epoch_id") or meta.get("portfolio_epoch_id") or "") != portfolio):
                raise PortfolioGuardUnavailable("PORT_GUARD_ORIGINAL_POSITION_SCOPE_INVALID")
            native = next(v for v in native_valuations if v["symbol"] == p["symbol"])
            bid = _decimal(native["mark_bid_usd"])
            limit = (bid * Decimal("0.998")).quantize(Decimal("0.01"), rounding=ROUND_FLOOR)
            if limit <= 0:
                raise PortfolioGuardUnavailable("PORT_GUARD_EXECUTABLE_PRICE_INVALID")
            original_sha = str(meta.get("entry_exit_contract_sha256") or "")
            original_version = str(meta.get("entry_exit_contract_version") or "")
            if not original_sha or not original_version:
                raise PortfolioGuardUnavailable("PORT_GUARD_ORIGINAL_BUY_CONTRACT_MISSING")
            guard_meta = {
                **meta,
                "portfolio_guard_action_key": p["action_key"],
                "portfolio_guard_policy_digest": manifest_digest,
                "portfolio_guard_policy_id": policy["policy_id"],
                "portfolio_guard_policy_version": policy["policy_version"],
                "portfolio_guard_episode_id": p["episode_id"],
                "portfolio_guard_stage_no": p["stage_no"],
                "portfolio_guard_portfolio_epoch_id": portfolio,
                "portfolio_guard_trading_epoch_id": epoch,
                "portfolio_guard_quote_asof_utc": native["quote_asof_utc"],
                "portfolio_guard_verified_bid_usd": native["mark_bid_usd"],
                "source_entry_contract_sha256": original_sha,
                "source_entry_contract_version": original_version,
                "position_lifecycle_id": cycle,
            }
            output.append({
                "symbol": p["symbol"], "exchange": resolve_quote_exchange(p["symbol"]),
                "side": "SELL", "qty": p["qty"], "available_qty": p["qty"],
                "limit_price": float(limit), "notional_usd": float(limit * p["qty"]),
                "strategy": "us_portfolio_profit_guard",
                "strategy_owner": "US_STANDARD",
                "exit_type": "portfolio_profit_guard",
                "exit_reason": "PORTFOLIO_PROFIT_GIVEBACK",
                "reason": "US_STANDARD_PROVEN_SIGNED_PORTFOLIO_GIVEBACK",
                "trade_date": trade_date,
                "client_order_key": p["action_key"],
                "semantic_action": "US_PORTFOLIO_GIVEBACK_STAGE_" + str(p["stage_no"]),
                "position_lifecycle_id": cycle, "position_cycle_id": cycle,
                "portfolio_epoch_id": portfolio, "trading_epoch_id": epoch,
                "broker_account_hash": account_hash,
                "partial_allowed": True, "meta": guard_meta,
            })
        return output
    except Exception as exc:
        logger.warning(
            "[US_PORT_GUARD][DATA_OR_RESERVATION_INVALID] error_type=%s action=no_portfolio_sell",
            type(exc).__name__,
        )
        return []


def portfolio_guard_close_health(*, env: str, offline: bool) -> dict:
    """Read-only Close statement; never treat an ACK as an executed SELL.

    OFF means no policy is active. With a signed active policy, every planned
    native order must have a FULL native cumulative fill proof before Close
    can say COMPLETE. UNKNOWN and missed remaining quantity are explicitly
    reported, without rescue POST / automatic cancellation.
    """
    if (
        os.getenv("US_PORTFOLIO_GUARD_ACTIVE", "0") != "1"
        or env != "practice" or offline
    ):
        return {"status": "OFF", "orders_checked": 0}
    from trader.account_state import get_account_key
    from trader.us.db.repos import _get_engine_or_none, _active_us_epoch
    from trader.us.execution.recovery_identity import current_us_portfolio_epoch
    try:
        raw_account = get_account_key(env="practice")
        engine = _get_engine_or_none()
        if not raw_account or engine is None:
            raise PortfolioGuardUnavailable("GUARD_CLOSE_ACCOUNT_OR_DB_MISSING")
        account_hash = hashlib.sha256(raw_account.encode()).hexdigest()
        now = datetime.now(timezone.utc)
        with engine.connect() as conn:
            epoch = str(_active_us_epoch(conn, required=True))
            portfolio = current_us_portfolio_epoch(
                conn, env="practice", account_key=raw_account,
                trading_epoch_id=epoch, strategy_owner="US_STANDARD",
            )
            authorized = _get_policy(
                conn, account_hash=account_hash, epoch=epoch,
                portfolio=portfolio, now=now,
            )
            if authorized is None:
                return {"status": "UNAUTHORIZED", "orders_checked": 0}
            policy, digest = authorized
            scope = {
                "policy_id": policy["policy_id"],
                "version": policy["policy_version"],
                "account": account_hash, "epoch": epoch, "portfolio": portfolio,
            }
            states = conn.execute(text("""
                SELECT stage_no,state,high_water_pnl_usd,latest_pnl_usd,
                       last_snapshot_at,episode_id
                FROM public.us_portfolio_guard_state
                WHERE policy_id=:policy_id AND policy_version=:version
                  AND env='practice' AND account_hash=:account
                  AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
            """), scope).mappings().all()
            if len(states) != 1:
                return {
                    "status": "BASELINE_OR_STATE_UNVERIFIED",
                    "orders_checked": 0,
                }
            state = dict(states[0])
            actions = conn.execute(text("""
                SELECT action_key,stage_no,target_qty,cumulative_verified_fill_qty,
                       state,manifest_sha256
                FROM public.us_portfolio_guard_actions
                WHERE policy_id=:policy_id AND policy_version=:version
                  AND env='practice' AND account_hash=:account
                  AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
            """), scope).mappings().all()
        outcomes = []
        for action in actions:
            if action["manifest_sha256"] != digest:
                raise PortfolioGuardUnavailable("GUARD_CLOSE_MANIFEST_MISMATCH")
            target = int(action["target_qty"])
            filled = int(action["cumulative_verified_fill_qty"])
            status = str(action["state"])
            if target <= 0 or filled < 0 or filled > target:
                raise PortfolioGuardUnavailable("GUARD_CLOSE_FILL_OVERFLOW")
            complete = status == "COMPLETE" and target == filled
            outcomes.append({
                "action_key": action["action_key"],
                "stage": int(action["stage_no"]),
                "target_qty": target,
                "native_verified_filled_qty": filled,
                "remaining_qty": target - filled,
                "complete": complete,
                "state": status,
            })
        stage_no = int(state["stage_no"])
        if stage_no > 0 and not outcomes:
            health = "TRIGGER_WITHOUT_NATIVE_ACTION_PROOF"
        elif any(not v["complete"] for v in outcomes):
            health = "REQUIRES_NATIVE_RECONCILIATION"
        elif outcomes:
            health = "COMPLETE"
        else:
            health = "WATCH"
        return {
            "status": health,
            "orders_checked": len(outcomes),
            "stage_no": stage_no,
            "hwm_usd": str(state["high_water_pnl_usd"]),
            "latest_pnl_usd": str(state["latest_pnl_usd"]),
            "last_snapshot_at": str(state["last_snapshot_at"]),
            "orders": outcomes,
        }
    except Exception as exc:
        logger.error(
            "[US_PORT_GUARD][CLOSE_READONLY_ERROR] type=%s action=REPORT_UNVERIFIED",
            type(exc).__name__,
        )
        return {"status": "UNVERIFIED", "orders_checked": 0,
                "manual_reconcile_required": True}

def assert_portfolio_guard_reentry_budget(
    intent: dict, *, order_notional_usd: Decimal, now: datetime,
) -> None:
    """An executed portfolio reduction cannot be instantly re-bought.

    An HMAC signed, cooldown-and-recovery-gated episode release is necessary.
    This does not override ordinary Minervini/VCP/1m entry conditions and
    cannot authorize any previously forbidden BUY. A missing DB is a fence.
    """
    if not (
        os.getenv("US_PORTFOLIO_GUARD_ACTIVE", "0") == "1"
        or os.getenv("US_PORTFOLIO_GUARD_REENTRY_GUARD_ENABLED", "0") == "1"
    ):
        return
    meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    if (intent.get("side") != "BUY"
            or str(intent.get("strategy_owner") or meta.get("strategy_owner") or "").upper()
                != "US_STANDARD"):
        return
    if os.getenv("KIS_ENV", "").lower() != "practice":
        raise PortfolioGuardUnavailable("GUARD_REENTRY_PRACTICE_ONLY")
    if now.tzinfo is None or _decimal(order_notional_usd) <= 0:
        raise PortfolioGuardUnavailable("GUARD_REENTRY_ORDER_VALUE_UNVERIFIED")
    if (
        os.getenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "0") != "1"
        or os.getenv("US_ACCOUNT_RESERVATION_MODE", "") != "ENFORCE"
    ):
        raise PortfolioGuardUnavailable("GUARD_REENTRY_SHARED_RESERVATION_REQUIRED")
    from trader.account_state import get_account_key
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none
    from trader.us.execution.recovery_identity import current_us_portfolio_epoch
    from trader.us.portfolio_profit_guard import verify_portfolio_policy, _utc
    import hmac
    raw_account = get_account_key(env="practice")
    if not raw_account:
        raise PortfolioGuardUnavailable("GUARD_REENTRY_ACCOUNT_MISSING")
    acct = hashlib.sha256(raw_account.encode()).hexdigest()
    engine = _get_engine_or_none()
    if engine is None:
        raise PortfolioGuardUnavailable("GUARD_REENTRY_DB_MISSING")
    with engine.begin() as conn:
        epoch = str(_active_us_epoch(conn, required=True))
        portfolio = current_us_portfolio_epoch(
            conn, env="practice", account_key=raw_account,
            trading_epoch_id=epoch, strategy_owner="US_STANDARD",
        )
        episodes = conn.execute(text("""
            SELECT s.policy_id,s.policy_version,s.episode_id,s.stage_no,
                   s.high_water_pnl_usd,s.latest_pnl_usd,s.last_snapshot_at,
                   s.episode_anchor,s.state,
                   p.authorization_json,p.authorization_sha256,
                   r.release_json,r.release_sha256
            FROM public.us_portfolio_guard_state s
            JOIN public.us_portfolio_guard_policy p
              ON p.policy_id=s.policy_id
             AND p.policy_version=s.policy_version
             AND p.env=s.env AND p.account_hash=s.account_hash
             AND p.trading_epoch_id=s.trading_epoch_id
             AND p.portfolio_epoch_id=s.portfolio_epoch_id
            LEFT JOIN public.us_portfolio_guard_reentry_release r
              ON r.policy_id=s.policy_id AND r.policy_version=s.policy_version
             AND r.env=s.env AND r.account_hash=s.account_hash
             AND r.trading_epoch_id=s.trading_epoch_id
             AND r.portfolio_epoch_id=s.portfolio_epoch_id
             AND r.episode_id=s.episode_id
            WHERE s.env='practice' AND s.account_hash=:account
              AND s.trading_epoch_id=:epoch AND s.portfolio_epoch_id=:portfolio
              AND s.episode_id IS NOT NULL
            FOR UPDATE OF s
        """), {"account": acct, "epoch": epoch, "portfolio": portfolio}).mappings().all()
        if not episodes:
            return
        if len(episodes) != 1:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_MULTIPLE_EPISODES_REQUIRE_REVIEW")
        state = dict(episodes[0])
        approved_policy, digest = verify_portfolio_policy(
            state["authorization_json"],
            signing_key=os.getenv("US_PORTFOLIO_GUARD_SIGNING_KEY", ""),
            account_hash=acct, trading_epoch_id=epoch,
            portfolio_epoch_id=portfolio, now_utc=now,
        )
        if digest != state["authorization_sha256"]:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_POLICY_DIGEST_MISMATCH")
        if state["state"] not in ("COMPLETE", "REARM_WAIT"):
            raise PortfolioGuardUnavailable("GUARD_REENTRY_STAGED_RISK_REDUCTION_ACTIVE")
        unsettled = conn.execute(text("""
            SELECT count(*) AS unresolved
            FROM public.us_portfolio_guard_actions
            WHERE env='practice' AND account_hash=:account
              AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
              AND policy_id=:policy AND policy_version=:version
              AND episode_id=:episode
              AND (state<>'COMPLETE' OR target_qty<>cumulative_verified_fill_qty)
        """), {"account":acct,"epoch":epoch,"portfolio":portfolio,
               "policy":state["policy_id"],"version":state["policy_version"],
               "episode":state["episode_id"]}).mappings().one()
        if int(unsettled["unresolved"] or 0) != 0:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_UNSETTLED_SELL")
        signed_release = state.get("release_json")
        if not isinstance(signed_release, dict):
            raise PortfolioGuardUnavailable("GUARD_REENTRY_OPERATOR_RELEASE_MISSING")
        release_body = {
            k: v for k, v in signed_release.items() if k != "hmac_sha256"
        }
        key = os.getenv("US_PORTFOLIO_GUARD_REARM_SIGNING_KEY", "")
        if len(key.encode()) < 32:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_RELEASE_KEY_MISSING")
        signed = hmac.new(
            key.encode(), canonical(release_body), hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(
            signed, str(signed_release.get("hmac_sha256") or ""),
        ):
            raise PortfolioGuardUnavailable("GUARD_REENTRY_RELEASE_SIGNATURE_INVALID")
        required = {
            "purpose": "US_STANDARD_PORTFOLIO_REENTRY_RELEASE_V1",
            "env": "practice", "sleeve": "US_STANDARD",
            "account_hash": acct, "trading_epoch_id": epoch,
            "portfolio_epoch_id": portfolio,
            "policy_id": state["policy_id"],
            "policy_version": state["policy_version"],
            "policy_sha256": digest,
            "episode_id": state["episode_id"],
        }
        if any(release_body.get(k) != v for k,v in required.items()):
            raise PortfolioGuardUnavailable("GUARD_REENTRY_RELEASE_SCOPE_INVALID")
        if hashlib.sha256(canonical(release_body)).hexdigest() != state["release_sha256"]:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_RELEASE_DIGEST_MISMATCH")
        start = _utc(release_body.get("effective_at"))
        expires = _utc(release_body.get("expires_at"))
        if not (start <= now.astimezone(timezone.utc) < expires):
            raise PortfolioGuardUnavailable("GUARD_REENTRY_RELEASE_EXPIRED")
        anchor = state.get("episode_anchor") or {}
        trigger_at = _utc(anchor.get("triggered_at_utc"))
        since = Decimal(str((now - trigger_at).total_seconds()))
        if since < _decimal(approved_policy["reentry_min_cooldown_seconds"]):
            raise PortfolioGuardUnavailable("GUARD_REENTRY_COOLDOWN_NOT_DONE")
        peak = _decimal(anchor.get("initial_hwm_usd"))
        triggered = _decimal(anchor.get("triggered_pnl_usd"))
        current = _decimal(state.get("latest_pnl_usd"))
        recovery_fraction = _decimal(
            approved_policy["reentry_min_recovery_fraction"]
        )
        if current < triggered + (peak - triggered) * recovery_fraction:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_PROFIT_NOT_RECOVERED")
        snapshot_age = Decimal(str((now - state["last_snapshot_at"]).total_seconds()))
        if snapshot_age < 0 or snapshot_age > _decimal(approved_policy["quote_ttl_sec"]):
            raise PortfolioGuardUnavailable("GUARD_REENTRY_PNL_SNAPSHOT_STALE")
        released_budget = _decimal(release_body.get("max_new_buy_notional_usd"))
        if released_budget <= 0:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_BUY_BUDGET_MISSING")
        # Serialized by the FOR UPDATE lock on the single guard episode
        # above. No two concurrent workers can consume the same released
        # dollars, and no unknown broker outcome is silently freed.
        key_value = str(intent.get("client_order_key") or "").strip()
        symbol_value = str(intent.get("symbol") or "").strip().upper()
        if not key_value or not symbol_value:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_BUY_IDENTITY_MISSING")
        prior_intent = conn.execute(text("""
            SELECT client_order_key,account_id,trading_epoch_id,
                   portfolio_epoch_id,side,symbol,notional_usd
            FROM public.us_order_intents
            WHERE client_order_key=:key AND side='BUY'
              AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
            LIMIT 2
        """), {"key":key_value,"epoch":epoch,"portfolio":portfolio}).mappings().all()
        if len(prior_intent) != 1:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_DURABLE_BUY_INTENT_MISSING")
        saved = prior_intent[0]
        if (
            str(saved.get("account_id") or "") not in (raw_account, acct)
            or str(saved.get("symbol") or "").strip().upper() != symbol_value
            or saved.get("notional_usd") is None
            or _decimal(saved["notional_usd"]) <= 0
            or _decimal(saved["notional_usd"]) > _decimal(order_notional_usd)
        ):
            raise PortfolioGuardUnavailable("GUARD_REENTRY_BUY_INTENT_SCOPE_OR_VALUE_INVALID")
        duplicate = conn.execute(text("""
            SELECT client_order_key
            FROM public.us_portfolio_guard_reentry_buy_reservations
            WHERE client_order_key=:key
        """), {"key":key_value}).first()
        if duplicate is not None:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_BUY_KEY_ALREADY_RESERVED")
        reserved = conn.execute(text("""
            SELECT COALESCE(SUM(notional_usd),0) AS amount
            FROM public.us_portfolio_guard_reentry_buy_reservations
            WHERE policy_id=:policy AND policy_version=:version
              AND env='practice' AND account_hash=:account
              AND trading_epoch_id=:epoch AND portfolio_epoch_id=:portfolio
              AND episode_id=:episode
        """), {
            "policy":state["policy_id"],"version":state["policy_version"],
            "account":acct,"epoch":epoch,"portfolio":portfolio,
            "episode":state["episode_id"],
        }).mappings().one()
        if _decimal(reserved["amount"]) + _decimal(order_notional_usd) > released_budget:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_RISK_BUDGET_EXCEEDED")
        inserted = conn.execute(text("""
            INSERT INTO public.us_portfolio_guard_reentry_buy_reservations
            (policy_id,policy_version,env,account_hash,trading_epoch_id,
             portfolio_epoch_id,episode_id,client_order_key,notional_usd)
            VALUES (:policy,:version,'practice',:account,:epoch,
                    :portfolio,:episode,:key,:amount)
            ON CONFLICT DO NOTHING
        """), {
            "policy":state["policy_id"],"version":state["policy_version"],
            "account":acct,"epoch":epoch,"portfolio":portfolio,
            "episode":state["episode_id"],"key":key_value,
            "amount":_decimal(order_notional_usd),
        })
        if inserted.rowcount != 1:
            raise PortfolioGuardUnavailable("GUARD_REENTRY_BUY_KEY_ALREADY_RESERVED")
