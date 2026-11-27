"""Bounded read-only hard-stop wakeup during the canonical US entry wait.

No order, DB mutation, soft-stop counter or HWM write in this module.
The original Tick owns canonical exit evaluation, claim and broker submission.
Only verified, uniquely owned swing positions are admitted. Missing collector
evidence leaves the ordinary exit path in charge.
"""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import math
import time


class ProtectiveEntryInterrupted(Exception):
    def __init__(self, snapshot):
        super().__init__("verified hard-stop observation interrupted BUY evaluation")
        self.snapshot = snapshot


def snapshot_still_fresh(snapshot, trade_date, now):
    from trader.us.market_calendar import is_us_regular_market_open
    try:
        age = time.time() - float(snapshot["exit_quote_evidence"]["asof_epoch"])
        return (now.date().isoformat() == str(trade_date)
                and is_us_regular_market_open(now) and 0 <= age <= 5)
    except (ValueError, TypeError, KeyError, OverflowError):
        return False


def make_hard_stop_poll(snapshots, *, excluded_symbols, trade_date, env, context):
    """Return a bounded observer; a trigger is a snapshot, never an intent."""
    from trader.us.entry_exit_contract import contract_exit_config, us_entry_exit_contract_integrity_state
    from trader.us.pb1.us_exit_router import normalize_book, normalize_horizon
    from trader.us.pb1.us_exit_engine import _resolve_position_entry_price
    from trader.us.market_calendar import now_ny
    from trader.marketdata.us_ws_collector import get_quote
    from trader.us.symbols import normalize_us_exchange
    if env != "practice":
        return None
    counts = Counter(str(p.get("symbol") or "").upper() for p in snapshots or [])
    candidates = []
    for pos in snapshots or []:
        symbol = str(pos.get("symbol") or "").upper()
        if (not symbol or symbol == "TQQQ" or counts[symbol] != 1
                or symbol in excluded_symbols or pos.get("exit_resolution_failed")
                or pos.get("legacy_nine_decision_only_provisional")
                or pos.get("strategy_owner") != "US_STANDARD"):
            continue
        try:
            meta = pos.get("meta") or {}
            if normalize_book(pos.get("book") or meta.get("book")) != "SWING_BOOK":
                continue
            if normalize_horizon(pos.get("horizon") or meta.get("horizon")) != "SWING_CARRY":
                continue
            # Require the original typed scope. No inferred lifecycle or owner.
            if not all(pos.get(k) for k in ("position_lifecycle_id", "position_cycle_id",
                    "portfolio_epoch_id", "trading_epoch_id", "broker_account_hash")):
                continue
            if pos["position_lifecycle_id"] != pos["position_cycle_id"]:
                continue
            if us_entry_exit_contract_integrity_state(pos, meta) != "VALID":
                continue
            if int(pos["orderable_qty"]) <= 0 or int(pos["qty"]) < int(pos["orderable_qty"]):
                continue
            entry = _resolve_position_entry_price(pos)
            hard = float(contract_exit_config(pos)["hard_stop"])
            if not (math.isfinite(entry) and entry > 0 and math.isfinite(hard) and 0 < hard < 1):
                continue
            candidates.append((deepcopy(pos),symbol,normalize_us_exchange(pos["exchange"]),entry,hard))
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
            continue
    if not candidates:
        return None
    cursor = 0

    def poll():
        nonlocal cursor
        if not context.broker_submit_allowed() or context.remaining_sec() < 10:
            return None
        now = now_ny()
        if now.date().isoformat() != str(trade_date):
            return None
        # Bounded IPC only. Round robin across all eligible holdings, no REST
        # burst, no concurrent broker authority, and no stale fallback.
        deadline = time.monotonic() + .8
        for _ in candidates:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            pos,symbol,exchange,entry,hard = candidates[cursor]
            cursor = (cursor + 1) % len(candidates)
            try:
                q = get_quote(symbol,exchange,env=env,max_age_sec=5,
                              timeout_sec=min(.15,remaining))
                if (not isinstance(q,dict) or q.get("source") != "KIS_WEBSOCKET_COLLECTOR"
                        or not q.get("collector_generation") or q.get("stale") or q.get("suspect")):
                    continue
                last = float(q["last"])
                received = float(q["received_at"])
                if not (math.isfinite(last) and last > 0 and 0 <= time.time()-received <= 5):
                    continue
                if (last-entry)/entry > -hard:
                    continue
                snapshot = deepcopy(pos)
                snapshot.update(resolved_current_price=last,current_price_usd=last,
                                current_price=last,resolved_pnl_pct=(last-entry)/entry,
                                resolved_quote_source=q["source"],resolved_quote_asof=received,
                                resolved_quote_received_at=datetime.now(timezone.utc).isoformat(),
                                exit_quote_evidence={"price":last,"source":q["source"],
                                    "asof_epoch":received,"quality":"FRESH",
                                    "received_at_utc":datetime.now(timezone.utc).isoformat()})
                if snapshot_still_fresh(snapshot,trade_date,now_ny()):
                    return snapshot
            except (ValueError,TypeError,KeyError,OverflowError):
                continue
        return None
    return poll
