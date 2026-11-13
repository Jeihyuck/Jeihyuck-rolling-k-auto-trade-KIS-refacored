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


def classify_skip(reason: str, *, proof: dict | None = None) -> tuple[str, str]:
    """Classify the observed reason, not the correctness of the underlying gate.

    Without independent account/KIS evidence an existing-holding or risk skip
    is unverified, even when the gate applied its configured code correctly.
    """
    token = str(reason or "").upper()
    detail = proof if isinstance(proof, dict) else {}
    failure = str(detail.get("failure_category") or "").upper()
    state = str(detail.get("confirmation_state") or "").upper()
    if token.startswith(("BUYABLE_EXISTING_HOLDING", "BUYABLE_DUPLICATE")):
        return "UNVERIFIED", "HOLDING_OR_DUPLICATE_NEEDS_BROKER_PROOF"
    if token.startswith("KR_VOLUME_PROOF_"):
        if failure == "STRATEGY_TRIGGER_NOT_MET" or (
            token.endswith(("_BREAKOUT_PRICE_TRIGGER_MISSING", "_BREAKOUT_VOLUME_TRIGGER_MISSING"))
        ):
            return "EXPECTED", "OWNER_TRIGGER_NOT_MET"
        if failure == "DATA_UNVERIFIED" or token.startswith((
            "KR_VOLUME_PROOF_KIS_", "KR_VOLUME_PROOF_COMPLETED_",
            "KR_VOLUME_PROOF_INSUFFICIENT_", "KR_VOLUME_PROOF_INVALID_",
        )):
            return "DATA_UNVERIFIED", "VOLUME_OR_QUOTE_PROOF"
        return "UNVERIFIED", "VOLUME_PROOF_DETAIL_MISSING"
    if token.startswith("KR_ENTRY_TIMING_"):
        return "UNVERIFIED", "TIMING_DETAIL_NOT_EVIDENCED"
    if token in {"NO_SIGNAL", "ENTRY_CONDITION_NOT_MET", "SETUP_FAILED"}:
        return "EXPECTED", "NO_SIGNAL_RECORDED"
    if token.startswith(("KR_MARKET_REGIME_", "RISK_")):
        return "UNVERIFIED", "OWNER_POLICY_AND_INPUT_NEED_AUDIT"
    return "UNVERIFIED", "OTHER_OR_NOT_CLASSIFIED"


def summarize_buy_skips(records: list[dict[str, Any]], *,
                        trade_date: str, max_rows: int | None = None) -> dict:
    counts = Counter()
    grouped = defaultdict(lambda: {"events": 0, "runs": set(), "reasons": Counter()})
    unique_evaluations = set()
    event_ids = set()
    missing_id_count = 0
    distinct_runs = set()
    different_stages = Counter()
    invalid_codes = 0
    for row in records:
        if str(row.get("side") or "").upper() != "BUY" or row.get("event_type") != "ORDER_SKIP":
            continue
        raw_code = str(row.get("code") or "").strip()
        valid_code = raw_code.isascii() and raw_code.isdecimal() and 1 <= len(raw_code) <= 6
        code = raw_code.zfill(6) if valid_code else "UNKNOWN_CODE"
        if not valid_code:
            invalid_codes += 1
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
        event_id = str(row.get("ledger_event_id") or "").strip()
        if event_id:
            event_ids.add(event_id)
        else:
            missing_id_count += 1
        unique_evaluations.add((run_id, code))
        payload = row.get("payload_json")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (ValueError, TypeError):
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
        proof = features.get("volume_proof")
        if not isinstance(proof, dict):
            proof = payload.get("volume_proof")
        for reason in reasons:
            category, label = classify_skip(reason, proof=proof)
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
        "distinct_ledger_event_ids": len(event_ids),
        "missing_event_id_count": missing_id_count,
        "event_id_duplicates_observed": (
            sum(different_stages.values()) - len(event_ids)
            if missing_id_count == 0 else None
        ),
        "unique_codes": len([code for code in grouped if code != "UNKNOWN_CODE"]),
        "invalid_or_missing_code_events": invalid_codes,
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
        "truncated": max_rows is not None and len(records) > max_rows,
        "unobserved_stages": "NOT_EVALUATED, not assumed PASS/FAIL",
    }
