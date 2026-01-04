from __future__ import annotations

import logging
from typing import Any, Dict

import trader.intent_store as intent_store
from trader.utils.env import env_bool
from strategy.types import ExecutionAck
from trader.config import (
    STRATEGY_ALLOW_SELL_ONLY,
    STRATEGY_DRY_RUN,
    STRATEGY_INTENTS_PATH,
    STRATEGY_INTENTS_STATE_PATH,
    STRATEGY_MAX_OPEN_INTENTS,
    STRATEGY_MODE,
)

logger = logging.getLogger(__name__)


class IntentExecutor:
    """Strategy intent executor (Phase 1: dry-run)."""

    def __init__(self) -> None:
        self.intents_path = STRATEGY_INTENTS_PATH
        self.cursor_state_path = STRATEGY_INTENTS_STATE_PATH
        self.allow_sell_only = STRATEGY_ALLOW_SELL_ONLY
        self.max_open_intents = int(STRATEGY_MAX_OPEN_INTENTS)

    def _should_dry_run(self) -> tuple[bool, str]:
        disable_live = env_bool("DISABLE_LIVE_TRADING", False)
        reasons = []
        if disable_live:
            reasons.append("DISABLE_LIVE_TRADING env=true")
        if STRATEGY_DRY_RUN:
            reasons.append("STRATEGY_DRY_RUN config=true")
        if STRATEGY_MODE == "INTENT_ONLY":
            reasons.append("STRATEGY_MODE=INTENT_ONLY")
        dry_run = bool(reasons)
        return dry_run, ",".join(reasons) if dry_run else ""

    def run_once(self) -> Dict[str, Any]:
        intents: list[Dict[str, Any]] = []
        cursor: Dict[str, Any] = {"offset": 0, "last_intent_id": None, "last_ts": None, "start_offset": 0}
        try:
            intents, cursor = intent_store.load_intents_since_cursor(
                self.intents_path, self.cursor_state_path
            )
            intents = intent_store.dedupe_intents(intents)
        except Exception:
            logger.exception("[INTENT_EXECUTOR] failed to load intents")
            return {"acks": [], "status": "error"}

        dry_run, dry_run_reason = self._should_dry_run()
        logger.info(
            "[INTENT_EXECUTOR] dry_run=%s reason=%s max_open_intents=%s",
            dry_run,
            dry_run_reason or "live",
            self.max_open_intents,
        )
        acks: list[ExecutionAck] = []
        processed = 0
        last_processed_offset = int(cursor.get("start_offset") or cursor.get("offset") or 0)
        last_intent_id = cursor.get("last_intent_id")
        last_ts = cursor.get("last_ts")
        for idx, intent in enumerate(intents):
            intent_id = intent.get("intent_id") or f"unknown-{idx}"
            side = str(intent.get("side") or "").upper()
            intent_offset = intent.get("_end_offset")

            if processed >= self.max_open_intents:
                acks.append(
                    ExecutionAck(
                        intent_id=intent_id,
                        ok=False,
                        message="max_open_intents_cap",
                        order_id=None,
                    )
                )
                break

            if self.allow_sell_only and side == "BUY":
                acks.append(
                    ExecutionAck(
                        intent_id=intent_id,
                        ok=False,
                        message="sell_only_mode",
                        order_id=None,
                    )
                )
                if intent_offset is not None:
                    try:
                        last_processed_offset = max(last_processed_offset, int(intent_offset))
                    except Exception:
                        pass
                processed += 1
                last_intent_id = intent_id
                last_ts = intent.get("ts") or last_ts
                continue

            if dry_run:
                logger.info(
                    "[INTENT_EXECUTOR][DRY_RUN] intent_id=%s side=%s symbol=%s qty=%s",
                    intent_id,
                    side,
                    intent.get("symbol"),
                    intent.get("qty"),
                )
                acks.append(
                    ExecutionAck(
                        intent_id=intent_id,
                        ok=True,
                        message="dry_run",
                        order_id=None,
                    )
                )
                processed += 1
                if intent_offset is not None:
                    try:
                        last_processed_offset = max(last_processed_offset, int(intent_offset))
                    except Exception:
                        pass
                last_intent_id = intent_id
                last_ts = intent.get("ts") or last_ts
                continue

            acks.append(
                ExecutionAck(intent_id=intent_id, ok=False, message="live_mode_not_implemented")
            )
            processed += 1
            if intent_offset is not None:
                try:
                    last_processed_offset = max(last_processed_offset, int(intent_offset))
                except Exception:
                    pass
            last_intent_id = intent_id
            last_ts = intent.get("ts") or last_ts

        intent_store.save_cursor(
            self.cursor_state_path,
            offset=last_processed_offset,
            last_intent_id=last_intent_id,
            last_ts=last_ts,
        )
        return {
            "acks": [ack.__dict__ for ack in acks],
            "processed": processed,
            "dry_run": dry_run,
            "dry_run_reason": dry_run_reason if dry_run else "",
        }
