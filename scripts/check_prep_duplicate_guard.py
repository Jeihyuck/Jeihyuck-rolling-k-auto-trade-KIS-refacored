#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from typing import Any

import pandas as pd

from trader.db.engine import get_engine
from trader.db.repos import load_final30_scored_db_only
from trader.final30_quality import build_canonical_prep_verdict, summarize_final30_quality
from trader.time_utils import now_kst, resolve_trade_context


def _flag_enabled(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _write_output(name: str, value: Any) -> None:
    output_path = os.getenv("GITHUB_OUTPUT")
    if not output_path:
        return
    with open(output_path, "a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def _load_final30_snapshot(env: str, as_of: str, strategy: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    try:
        df = load_final30_scored_db_only(
            get_engine(),
            env=env,
            as_of=as_of,
            strategy=strategy,
            require_exact_rows=30,
            fail_if_missing=False,
        )
    except Exception as exc:
        return pd.DataFrame(), {"status": "MISSING", "quality_ok": 0, "trade_can_proceed": 0, "error": str(exc)}

    if df.empty or len(df) != 30:
        return df, {"status": "MISSING", "quality_ok": 0, "trade_can_proceed": 0}

    quality = summarize_final30_quality(df, required_rows=30)
    verdict = build_canonical_prep_verdict(
        quality=quality,
        flow_failed_ratio=0.0,
        flow_fail_reason_counts={},
    )
    return df, verdict


def main() -> int:
    env = (os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice").strip().lower() or "practice"
    strategy = os.getenv("WATCHLIST_FINAL_STRATEGY_KEY") or "pb1_watchlist_final_scored"
    event_name = (os.getenv("GITHUB_EVENT_NAME") or "unknown").strip().lower() or "unknown"
    allow_duplicate = _flag_enabled(os.getenv("ALLOW_DUPLICATE_PREP"))
    ctx = resolve_trade_context(now=now_kst(), env=env)
    as_of = str(ctx.get("as_of") or "")

    df, verdict = _load_final30_snapshot(env=env, as_of=as_of, strategy=strategy)
    final30_count = int(len(df.index)) if not df.empty else 0
    quality_ok = int(verdict.get("quality_ok") or 0)
    trade_can_proceed = int(verdict.get("trade_can_proceed") or 0)
    canonical_status = str(verdict.get("status") or "MISSING").upper()
    is_ready = (
        final30_count == 30
        and quality_ok == 1
        and trade_can_proceed == 1
        and canonical_status != "FAIL"
    )
    should_skip = int(is_ready and not allow_duplicate)
    skip_reason = "canonical_prep_already_ready" if should_skip else ""

    print(
        "[PREP][DUPLICATE_GUARD][CHECK] "
        f"as_of={as_of} env={env} final30_count={final30_count} quality_ok={quality_ok} "
        f"trade_can_proceed={trade_can_proceed} canonical_status={canonical_status} "
        f"allow_duplicate={int(allow_duplicate)}"
    )

    if should_skip:
        print(f"[PREP][DUPLICATE_GUARD][SKIP] as_of={as_of} reason={skip_reason}")
        print(
            "[RUN_SUMMARY][RESULT] "
            f"status=SKIP_DUPLICATE_PREP reason={skip_reason} event={event_name}"
        )
    elif allow_duplicate and is_ready:
        print(
            "[PREP][DUPLICATE_GUARD][BYPASS] "
            f"as_of={as_of} reason=allow_duplicate_prep_requested"
        )
    else:
        print(
            "[PREP][DUPLICATE_GUARD][RUN] "
            f"as_of={as_of} reason={verdict.get('error') or 'canonical_prep_not_ready'}"
        )

    _write_output("as_of", as_of)
    _write_output("final30_count", final30_count)
    _write_output("quality_ok", quality_ok)
    _write_output("trade_can_proceed", trade_can_proceed)
    _write_output("canonical_status", canonical_status)
    _write_output("should_skip", should_skip)
    _write_output("skip_reason", skip_reason)
    return 0


if __name__ == "__main__":
    sys.exit(main())