# -*- coding: utf-8 -*-
"""Small pure helpers for the US session runner."""
from __future__ import annotations


def attribute_session_fills(
    orders: list[dict],
    broker_fills: list[dict],
    *,
    session: str,
    session_run_id: str,
) -> dict:
    """Attribute cumulative broker evidence only to orders submitted by this session."""
    session_orders = [
        row
        for row in orders or []
        if str(row.get("session") or "").lower() == session.lower()
        and str(row.get("session_run_id") or "") == str(session_run_id)
    ]

    def identities(row: dict) -> tuple[str, str, str]:
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        return (
            str(row.get("canonical_order_no") or row.get("canonical_broker_order_no") or row.get("order_no") or ""),
            str(row.get("submit_attempt_id") or meta.get("submit_attempt_id") or ""),
            str(row.get("client_order_key") or meta.get("client_order_key") or ""),
        )

    fill_ids = [identities(row) for row in broker_fills or [] if int(row.get("filled_qty") or row.get("qty") or 0) > 0]
    matched = 0
    unresolved = 0
    seen_orders: set[tuple[str, str, str]] = set()
    for order in session_orders:
        wanted = identities(order)
        dedupe_key = next(((value, "", "") for value in wanted if value), wanted)
        if dedupe_key in seen_orders:
            continue
        seen_orders.add(dedupe_key)
        found = any(any(value and value == evidence[index] for index, value in enumerate(wanted)) for evidence in fill_ids)
        matched += int(found)
        unresolved += int(not found)
    return {"session_fills_count": matched, "unresolved_order_count": unresolved}
