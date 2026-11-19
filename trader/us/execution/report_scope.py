"""Identity provenance for comparing read-only daily order/fill populations."""
from __future__ import annotations


def population_scope(rows: list[dict], trade_date: str) -> dict:
    identities = set()
    for row in rows:
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        account = str(row.get("account_id") or row.get("broker_account_hash") or meta.get("broker_account_hash") or "")
        epoch = str(row.get("trading_epoch_id") or meta.get("trading_epoch_id") or "")
        source_date = str(row.get("trade_date") or "")
        if not account or not epoch or source_date != str(trade_date):
            return {"verified": False, "trade_date": str(trade_date), "reason": "INCOMPLETE_SOURCE_SCOPE"}
        identities.add((account, epoch, source_date))
    if len(identities) != 1:
        return {"verified": False, "trade_date": str(trade_date), "reason": "EMPTY_OR_MIXED_SOURCE_SCOPE"}
    account, epoch, source_date = next(iter(identities))
    return {"verified": True, "account_id": account, "trading_epoch_id": epoch, "trade_date": source_date}
