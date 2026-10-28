"""Canonical helpers for deciding whether a US PREP result is operationally usable.

Entry quality and session/exit liveness are intentionally separate.  A PREP that
completed successfully but blocks new BUYs (cluster cap, underfill, risk/data
quality) is still an effective same-day contract and must not be overwritten by
recovery with a newer STARTED row.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any


# These are completed PREP states, not permission to open new BUYs.  Do not
# accept arbitrary *_BLOCKED states: a system or provider failure must remain
# ineligible for reuse even if its label contains ENTRY_BLOCKED.
_EXIT_ONLY_COMPLETE_STATUSES = frozenset({
    "RISK_OFF_ENTRY_BLOCKED",
    "DEFENSE_CRASH_ENTRY_BLOCKED",
})


def is_effective_prep_status(status: object) -> bool:
    """Recognize completion independently of the new-entry policy."""
    text = str(status or "").strip().upper()
    return (
        text in {"OK", "OK_WITH_WARNINGS"}
        or text.startswith("OK_WITH_WARNINGS_")
        or text in _EXIT_ONLY_COMPLETE_STATUSES
    )


def locked_rows_match_prep_run(
    prep: dict[str, Any] | None,
    rows: Sequence[Any] | None,
) -> bool:
    """Bind the DB locked watchlist to the PREP run that claims completion.

    Same-day rows from an older PREP must never validate a newer completed PREP.
    Every locked row is therefore required to carry the exact completed run_id.
    Missing/blank run ids fail closed.
    """
    if not isinstance(prep, dict):
        return False
    expected_run_id = str(prep.get("run_id") or "").strip()
    if not expected_run_id or not rows:
        return False
    for row in rows:
        if not isinstance(row, dict):
            return False
        if str(row.get("run_id") or "").strip() != expected_run_id:
            return False
    return True




def db_prep_entry_permission(status: object, result: dict[str, Any] | None) -> tuple[bool, str]:
    """A completed PREP is not automatically a BUY-authorizing PREP.

    The exit/close liveness contract may be valid while new entries remain
    prohibited. Never equate trade_can_proceed=1 to entry_can_proceed=1.
    """
    data = result if isinstance(result, dict) else {}
    normalized = str(status or "").strip().upper()
    if normalized not in {"OK", "OK_WITH_WARNINGS"} and not normalized.startswith("OK_WITH_WARNINGS_"):
        return False, "prep_completion_not_entry_eligible"
    for key in ("entry_can_proceed", "exit_can_proceed", "trade_can_proceed",
                "final30_trade_ready", "contract_ok", "allow_new_buy"):
        value = data.get(key)
        if value not in (1, True, "1", "true", "True"):
            return False, f"db_entry_contract_{key}_not_proven"
    if (data.get("contract_version") != "us_sector_rotation_v3"
            or data.get("market_regime_version") != "us_leading_regime_v1"):
        return False, "db_entry_contract_version_unverified"
    if data.get("force_entry_block") in (1, True, "1", "true", "True"):
        return False, "db_entry_force_block"
    if str(data.get("trade_block_reason") or "ok").lower() not in {
        "ok", "ok_crash_rebound_limited",
    }:
        return False, "db_entry_trade_block_reason"
    return True, "ok_db_entry_contract_verified"


def resolve_session_prep_cache(
    latest_attempt: dict[str, Any] | None,
    completed_candidate: dict[str, Any] | None,
    locked_rows: Sequence[Any] | None,
) -> tuple[dict[str, Any], str]:
    """Choose only a completed, exact-run cache for order-capable Tick children.

    Do not turn STARTED into OK or silently borrow rows from another PREP.
    This function is data selection only: session/preflight permission remains
    independently authoritative, including code SHA and EXIT_ONLY markers.
    """
    latest = dict(latest_attempt or {})
    if is_effective_prep_status(latest.get("status")):
        return latest, "latest_effective"
    completed = dict(completed_candidate or {})
    if (is_effective_prep_status(completed.get("status"))
            and completed.get("finished_at") is not None
            and locked_rows_match_prep_run(completed, locked_rows)):
        return completed, "completed_same_run_proven"
    return latest, "unverified_incomplete"



def authorize_recovered_prep_guard(
    *,
    candidate: dict[str, Any] | None,
    locked_rows: Sequence[Any] | None,
    fresh_guard: dict[str, Any] | None,
    trade_date: str,
    current_revision: str,
    manifest_revision_verified: bool,
) -> tuple[bool, str]:
    """Never promote a cached PREP without a newly proven session guard.

    An old STARTED cache, valid EXIT_ONLY record, mismatched selected watchlist,
    unpinned code or preflight marker must never authorize a new BUY.
    """
    prep = candidate if isinstance(candidate, dict) else {}
    guard = fresh_guard if isinstance(fresh_guard, dict) else {}
    if not manifest_revision_verified or not current_revision or current_revision == "unknown":
        return False, "run_manifest_revision_unverified"
    if str(prep.get("trade_date") or trade_date) != trade_date:
        return False, "recovery_prep_trade_date_mismatch"
    if not is_effective_prep_status(prep.get("status")):
        return False, "recovery_prep_not_completed"
    if not locked_rows_match_prep_run(prep, locked_rows):
        return False, "recovery_prep_watchlist_identity_unverified"
    data = prep.get("result") if isinstance(prep.get("result"), dict) else {}
    allowed, reason = db_prep_entry_permission(prep.get("status"), data)
    if not allowed:
        return False, reason
    if guard.get("ok") is not True or guard.get("entry_can_proceed") is not True:
        return False, "recovery_fresh_guard_not_entry_authorized"
    if str(guard.get("guard_state") or "").upper() != "PREP_OK":
        return False, "recovery_guard_state_not_prep_ok"
    if not bool(guard.get("exit_can_proceed", False)):
        return False, "recovery_exit_liveness_unverified"
    if bool(guard.get("version_mismatch") or guard.get("run_revision_mismatch")):
        return False, "recovery_run_revision_mismatch"
    if str(data.get("run_revision") or data.get("git_commit_sha") or "") != current_revision:
        return False, "recovery_candidate_revision_mismatch"
    guard_contract = guard.get("contract") if isinstance(guard.get("contract"), dict) else {}
    guard_run = str(guard_contract.get("run_id") or guard.get("run_id") or "").strip()
    if not guard_run or guard_run != str(prep.get("run_id") or "").strip():
        return False, "recovery_guard_candidate_run_mismatch"
    return True, "recovered_prep_contract_and_guard_verified"

def is_effective_prep_contract(
    contract: dict[str, Any] | None,
    rows: Sequence[Any] | None,
    *,
    trade_date: str | None = None,
    min_rows: int = 10,
) -> bool:
    """Return whether a same-day PREP artifact is safe to reuse.

    This answers only whether recovery should regard PREP as completed.  It does
    *not* re-enable entries.  ``entry_can_proceed`` may legitimately be false;
    existing-position exit/close liveness remains authoritative.
    """
    if not isinstance(contract, dict):
        return False
    if trade_date and str(contract.get("trade_date") or "") not in {"", str(trade_date)}:
        return False
    if not is_effective_prep_status(contract.get("status")):
        return False
    if len(rows or ()) < max(1, int(min_rows)):
        return False

    trade_can_proceed = bool(int(contract.get("trade_can_proceed", 0) or 0))
    contract_ok = bool(contract.get("contract_ok", trade_can_proceed))
    exit_can_proceed = bool(int(contract.get("exit_can_proceed", trade_can_proceed) or 0))
    if not contract_ok or not exit_can_proceed:
        return False

    row_count = len(rows or ())
    declared_count = int(contract.get("final30_scored_count", row_count) or row_count)
    score_nonzero_count = int(contract.get("score_nonzero_count", declared_count) or 0)
    if declared_count > 0 and declared_count != row_count:
        return False
    if score_nonzero_count != declared_count:
        return False
    return True


def check_effective_prep_sources(trade_date: str, *, min_rows: int = 10) -> dict[str, Any]:
    """One read-only artifact/DB/provenance verdict for all WSL PREP gates.

    Recovery may skip rebuilding only when preflight would accept these exact
    sources. Missing identity never falls back to a newer/unrelated DB run.
    Entry-blocked completed contracts remain completed without granting BUYs.
    """
    from trader.us.path_contract import load_us_final30_scored, load_us_prep_contract
    from trader.us.db.repos import load_us_prep_completion_for_run, load_locked_us_watchlist
    from trader.us.score_columns import validate_us_entry_provenance_contract

    contract = load_us_prep_contract(trade_date) or {}
    artifact_rows = load_us_final30_scored(trade_date) or []
    run_id = str(contract.get("run_id") or "").strip()
    result = {"ok": False, "trade_date": trade_date, "reason": "artifact_contract_invalid"}
    if not is_effective_prep_contract(contract, artifact_rows, trade_date=trade_date, min_rows=min_rows):
        return result
    if not run_id:
        return {**result, "reason": "artifact_run_id_missing"}
    prep = load_us_prep_completion_for_run(trade_date, run_id) or {}
    locked_rows = load_locked_us_watchlist(trade_date=trade_date, min_count=min_rows, allow_degraded=True) or []
    provenance = validate_us_entry_provenance_contract(locked_rows)
    same_run = run_id == str(prep.get("run_id") or "").strip()
    ok = (
        same_run and is_effective_prep_status(prep.get("status"))
        and len(locked_rows) >= min_rows
        and provenance.get("ok") is True
        and locked_rows_match_prep_run(prep, locked_rows)
    )
    return {**result, "ok": bool(ok), "reason": "ok" if ok else "db_live_contract_invalid",
            "status": contract.get("status"), "artifact_count": len(artifact_rows),
            "locked_count": len(locked_rows), "provenance_ok": provenance.get("ok") is True}
