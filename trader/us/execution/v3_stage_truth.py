"""Read exact durable v3 stage truth from account-scoped execution claims.

No mutation, claim release, KIS call or terminal-state inference occurs here.
A prior Trading Epoch's identical original portfolio/cycle is considered so
a new session cannot sell the one-time partial again.
"""
from __future__ import annotations

import re

from sqlalchemy import text


V3_ACTIONS = ("PROFIT_DEFENSE_PARTIAL", "RUNNER_FINAL")


def classify_v3_claim_rows(rows: list[dict] | None) -> dict[str, str]:
    if rows is None:
        return {stage: "UNKNOWN" for stage in V3_ACTIONS}
    status = {stage: "NOT_TRIGGERED" for stage in V3_ACTIONS}
    for row in rows:
        action = str(row.get("action") or "").upper()
        state = str(row.get("action_state") or "").upper()
        if action not in status:
            continue
        new = "DONE" if state == "SATISFIED" else "ACTIVE_ORDER" if state in {
            "OPEN", "IN_FLIGHT", "UNCERTAIN", "PARTIALLY_SATISFIED"
        } else "BLOCKED"
        # Any unverified/broker active state is more conservative than DONE,
        # even if contradictory rows are present in two epochs.
        precedence = {"NOT_TRIGGERED": 0, "DONE": 1, "ACTIVE_ORDER": 2, "BLOCKED": 3}
        if precedence[new] > precedence[status[action]]:
            status[action] = new
    return status


def read_v3_lifecycle_stage_states(
    *, lifecycle_id: str, position_cycle_id: str,
    portfolio_epoch_id: str, contract_sha: str,
) -> dict[str, str]:
    unknown = {stage: "UNKNOWN" for stage in V3_ACTIONS}
    if not all((lifecycle_id, position_cycle_id, portfolio_epoch_id)) or not re.fullmatch(
        r"[a-f0-9]{64}", str(contract_sha or "")
    ):
        return unknown
    try:
        from trader.us.db.repos import _get_engine_or_none, _us_execution_claim_scope
        scope = _us_execution_claim_scope()
        if scope.get("env") != "practice" or not scope.get("account_id") or not scope.get("trading_epoch_id"):
            return unknown
        engine = _get_engine_or_none()
        if engine is None:
            return unknown
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT action, action_state FROM us_execution_claims
                 WHERE env='practice' AND market='US'
                   AND account_id=:account AND strategy_owner='US_STANDARD'
                   AND lifecycle_id=:lifecycle AND position_cycle_id=:cycle
                   AND portfolio_epoch_id=:portfolio
                   AND action_instance=:sha
                   AND action IN ('PROFIT_DEFENSE_PARTIAL','RUNNER_FINAL')
            """), {
                "account": scope["account_id"], "lifecycle": lifecycle_id,
                "cycle": position_cycle_id, "portfolio": portfolio_epoch_id,
                "sha": contract_sha,
            }).mappings().all()
        return classify_v3_claim_rows([dict(row) for row in rows])
    except Exception:
        # A DB error can never imply NOT_TRIGGERED.
        return unknown
