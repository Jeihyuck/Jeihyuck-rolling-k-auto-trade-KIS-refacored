from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _is_before_pm_close(now: datetime | None, phase: str | None) -> bool:
    if now is None:
        return str(phase or "").strip().lower() == "pm_entry"
    return now.strftime("%H:%M") < "15:10"


def validate_run_pipeline(
    *,
    session_kind: str,
    phase: str,
    final30_rows: int,
    entry_enabled: bool,
    exit_only: bool,
    mode_applied: bool,
    entry_decision_seen: bool,
    setup_ok_seen: bool,
    order_candidates_seen: bool,
    buyable_gate_seen: bool,
    order_submit_seen: bool,
    no_buy_reason: str | None = None,
    top_blockers: dict[str, int] | str | None = None,
    order_candidates_count: int = 0,
    now: datetime | None = None,
    artifact_dir: str | Path = "artifacts",
) -> dict[str, Any]:
    session = str(session_kind or "unknown").strip().lower()
    phase_n = str(phase or "unknown").strip().lower()
    entry_pipeline_seen = bool(entry_decision_seen or setup_ok_seen or order_candidates_seen)
    status = "OK"
    reason = "ok"
    if (
        session in {"afternoon", "pm"}
        and _is_before_pm_close(now, phase_n)
        and int(final30_rows or 0) > 0
        and bool(entry_enabled)
        and not bool(exit_only)
        and bool(mode_applied)
        and not entry_pipeline_seen
    ):
        status = "ERROR"
        reason = "ENTRY_PIPELINE_NOT_SEEN_AFTER_MODE_APPLIED"
        logger.error("[RUN_VALIDATOR][ERROR] reason=%s", reason)
    elif bool(entry_enabled) and int(final30_rows or 0) > 0 and not entry_pipeline_seen:
        status = "WARN"
        reason = "entry_pipeline_not_seen"

    if isinstance(top_blockers, str):
        parsed_blockers: dict[str, int] | str = top_blockers
    else:
        parsed_blockers = dict(top_blockers or {})

    payload: dict[str, Any] = {
        "session": session,
        "phase": phase_n,
        "final30_rows": int(final30_rows or 0),
        "entry_enabled": bool(entry_enabled),
        "exit_only": bool(exit_only),
        "mode_applied": bool(mode_applied),
        "entry_pipeline_seen": entry_pipeline_seen,
        "entry_decision_seen": bool(entry_decision_seen),
        "setup_ok_seen": bool(setup_ok_seen),
        "order_candidates_seen": bool(order_candidates_seen),
        "buyable_gate_seen": bool(buyable_gate_seen),
        "order_submit_seen": bool(order_submit_seen),
        "order_candidates_count": int(order_candidates_count or 0),
        "no_buy_reason": no_buy_reason or "",
        "top_blockers": parsed_blockers,
        "status": status,
        "reason": reason,
    }
    out_dir = Path(artifact_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "run_validation_summary.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    logger.info(
        "[RUN_VALIDATOR][SUMMARY] session=%s entry_enabled=%s exit_only=%s final30_rows=%s entry_pipeline_seen=%s status=%s reason=%s",
        session,
        int(bool(entry_enabled)),
        int(bool(exit_only)),
        int(final30_rows or 0),
        int(entry_pipeline_seen),
        status,
        reason,
    )
    logger.info("[RUN_VALIDATOR][JSON] path=%s", path)
    return payload
