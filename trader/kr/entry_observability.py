"""Read-only KR PB1 BUY skip classification for operator audits.

A skip is evidence of an observed gate result, NOT evidence of an
implementation bug. Absence of a gate evaluation is NOT_EVALUATED.
No orders, mutations, credentials, KIS calls or epoch rewriting.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import json
from typing import Any


def reasons_from_ledger(raw: Any) -> list[str]:
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, str):
        try:
            values = json.loads(raw)
        except (TypeError, ValueError):
            values = [raw]
    else:
        values = []
    return [str(value) for value in values if
            str(value).strip() and not str(value).startswith("name:")]


def classify_skip(reason: str) -> tuple[str, str]:
    token = str(reason or "").upper()
    if token.startswith("BUYABLE_EXISTING_HOLDING") or token.startswith("BUYABLE_DUPLICATE"):
        return "EXPECTED", "EXISTING_POSITION_OR_DUPLICATE"
    if token.startswith("KR_VOLUME_PROOF_") or any(
        substring in token for substring in ("QUOTE_STALE", "SOURCE_UNVERIFIED", "PROOF_MISSING")
    ):
        return "DATA_UNVERIFIED", "VOLUME_OR_QUOTE_PROOF"
    if token.startswith("KR_ENTRY_TIMING_"):
        # Context may contain WAIT, UNKNOWN, expired proof or a price-cap hit.
        # An opaque ledger reason code alone cannot resolve that distinction.
        return "UNVERIFIED", "TIMING_DETAIL_NOT_EVIDENCED"
    if token in {"NO_SIGNAL", "ENTRY_CONDITION_NOT_MET", "SETUP_FAILED"}:
        return "EXPECTED", "NO_SIGNAL"
    if token.startswith("KR_MARKET_REGIME_") or token.startswith("RISK_"):
        return "EXPECTED", "POLICY_GATE"
    return "UNVERIFIED", "OTHER_OR_NOT_CLASSIFIED"


def summarize_buy_skips(records: list[dict[str, Any]], *,
                        trade_date: str, max_rows: int | None = None) -> dict:
    counts = Counter()
    grouped = defaultdict(lambda: {"events": 0, "runs": set(), "reasons": Counter()})
    unique_evaluations = set()
    event_ids = set()
    distinct_runs = set()
    different_stages = Counter()
    for row in records:
        if str(row.get("side") or "").upper() != "BUY" or row.get("event_type") != "ORDER_SKIP":
            continue
        code = str(row.get("code") or "").zfill(6)
        run_id = str(row.get("run_id") or "")
        stage = str(row.get("stage") or "UNKNOWN_STAGE")
        distinct_runs.add(run_id)
        different_stages[stage] += 1
        reasons = reasons_from_ledger(row.get("reasons"))
        if not reasons:
            reasons = ["UNSPECIFIED_SKIP"]
        # Multiple reasons are separate evidence; a single ledger event is
        # still counted once. Do not attribute all rejections to the timing gate.
        grouped[code]["events"] += 1
        grouped[code]["runs"].add(run_id)
        event_ids.add((run_id, stage, code, str(row.get("ts") or ""), tuple(reasons)))
        unique_evaluations.add((run_id, code))
        for reason in reasons:
            category, label = classify_skip(reason)
            counts[(category, label, reason)] += 1
            grouped[code]["reasons"][reason] += 1
    categories = Counter()
    for (category, _, _), n in counts.items():
        categories[category] += n
    return {
        "trade_date_kst": trade_date,
        "report_scope": "KR PB1 practice run JOIN; account/epoch not independently attested",
        "broker_holdings_verified": False,
        "timing_gate_evaluation_verified": False,
        "event_count": sum(different_stages.values()),
        "deduplicated_event_keys": len(event_ids),
        "unique_codes": len(grouped),
        "unique_run_code_pairs": len(unique_evaluations),
        "unique_run_ids": len(distinct_runs),
        "by_stage": dict(sorted(different_stages.items())),
        "by_category_reason_occurrences": dict(sorted(categories.items())),
        "by_reason": [
            {"category": category, "group": label,
             "reason": reason, "occurrences": n}
            for (category, label, reason), n in counts.most_common()
        ],
        "by_code": {
            code: {"events": item["events"], "unique_runs": len(item["runs"]),
                   "reasons": dict(item["reasons"])}
            for code, item in sorted(grouped.items())
        },
        "truncated": max_rows is not None and len(records) >= max_rows,
        "unobserved_stages": "NOT_EVALUATED, not assumed PASS/FAIL",
    }
