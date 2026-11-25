"""Read-only KR close settlement audit.

ACK is never a fill. Broker holdings at zero must not silently imply native KIS
execution price, and a manually closed DB position remains economically pending
until every SELL order is matched to durable, priced execution evidence.
No writes to broker, orders, fills, or positions are performed here.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa


def _number(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0



def _broker_confirmed_nonexecution(order: dict, requested: int) -> int:
    """Count only KIS-proven rejected/cancelled shares, never infer from status.

    An order ACK, terminal DB state, or empty broker holdings is insufficient.
    Verify that one native daily-ccld row identifies this exact SELL order,
    accounts for every requested share, and has no live remaining quantity.
    """
    raw = order.get("response_json") or order.get("broker_confirmation") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return 0
    if not isinstance(raw, dict):
        return 0
    row = raw.get("kis_row") or (
        (raw.get("_reconcile_observation") or {}).get("kis_row")
        if isinstance(raw.get("_reconcile_observation"), dict) else None
    )
    if not isinstance(row, dict):
        # Root-level raw KIS daily-ccld rows are also acceptable.
        row = raw
    status = str(order.get("status") or "").upper()
    terminal_statuses = {
        "CANCELED", "CANCELLED", "REJECTED", "PARTIAL_CANCELED",
        "PARTIALLY_CANCELED", "PARTIAL_CANCELLED", "RECONCILED",
        "PARTIAL_FILLED",
    }
    if status not in terminal_statuses:
        return 0
    order_no = str(order.get("kis_odno") or "").strip()
    row_no = str(row.get("odno") or "").strip()
    if not order_no or not row_no or order_no.zfill(10) != row_no.zfill(10):
        return 0
    if str(row.get("sll_buy_dvsn_cd") or "") != "01":
        return 0
    if str(order.get("code") or "").zfill(6) != str(row.get("pdno") or "").zfill(6):
        return 0
    required = ("ord_qty", "tot_ccld_qty", "cncl_cfrm_qty", "rjct_qty", "rmn_qty")
    if any(row.get(key) in (None, "") for key in required):
        return 0
    values = {}
    for key in required:
        try:
            value = int(str(row[key]).strip())
        except (ValueError, TypeError):
            return 0
        if value < 0:
            return 0
        values[key] = value
    cancelled = values["cncl_cfrm_qty"]
    rejected = values["rjct_qty"]
    executed = values["tot_ccld_qty"]
    if values["ord_qty"] != requested or values["rmn_qty"] != 0:
        return 0
    if not cancelled and not rejected:
        return 0
    if executed + cancelled + rejected != requested:
        return 0
    return cancelled + rejected



def _explicit_kr_preack_rejection_not_a_broker_order(
    order: dict, related_fills: list[dict],
) -> bool:
    """Exclude only native KIS *pre-ACK* denials from priced SELL accounting.

    A real broker order with ODNO, any fill row, ambiguous HTTP response,
    cancelled/partial order or unsigned contract stays in the economic audit.
    This never marks an economic SELL as FILLED or changes a DB row.
    """
    if (
        str(order.get("status") or "").upper() != "ERROR"
        or str(order.get("side") or "").upper() != "SELL"
        or related_fills
        or order.get("kis_odno")
        or order.get("acked_at")
    ):
        return False
    client_key = str(order.get("client_order_key") or "").strip()
    if not client_key or order.get("broker_order_id") not in (None, "", client_key):
        return False

    def _obj(raw: Any) -> dict:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                decoded = json.loads(raw)
                return decoded if isinstance(decoded, dict) else {}
            except (TypeError, ValueError):
                pass
        return {}

    request = _obj(order.get("request_json"))
    response = _obj(order.get("response_json"))
    contract = _obj(request.get("kr_us_tp_contract"))
    stage = str(request.get("kr_us_tp_stage") or "")
    if stage not in {"TP1", "TP2", "TP3", "PROFIT_DEFENSE_PARTIAL", "RUNNER_FINAL"}:
        return False
    from trader.kr.us_standard_profit_parity import verified_contract
    from trader.kr.us_profit_stage_journal import _explicit_kis_preack_reject
    if (
        not verified_contract(contract)
        or contract.get("activation_mode") != "ACTIVE_PRACTICE"
        or contract.get("code") != str(order.get("code") or "").zfill(6)
        or str(request.get("strategy_owner") or "") != "PB1"
    ):
        return False
    return _explicit_kis_preack_reject(
        response, code=str(order.get("code") or ""), qty=order.get("qty"),
    )


def _broker_order_day(value: Any) -> date | None:
    if isinstance(value, datetime):
        timestamp = value
    elif isinstance(value, date):
        return value
    elif isinstance(value, str):
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if timestamp.tzinfo is None:
        # A naive date cannot identify a broker trade-day boundary.
        return None
    return timestamp.astimezone(timezone(timedelta(hours=9))).date()


def _stored_infinite_sell_proof(order: dict, candidates: list[dict]) -> dict | None:
    """Verify a legacy PB1 mirror against the dedicated owner journal.

    A previous cycle need not remain the current KR_INFINITE state row.
    Stored native economics are not a fresh broker query. Never insert a
    second PB1 fill for a proven owner SELL or change current sleeve state.
    """
    from decimal import Decimal, InvalidOperation
    if (order.get("code") != "122630" or order.get("side") != "SELL"
            or order.get("status") != "FILLED" or not order.get("order_id")
            or order.get("position_cycle_id")):
        return None
    day = _broker_order_day(order.get("created_at"))
    if day is None:
        return None
    odno = str(order.get("kis_odno") or "")
    if len(odno) != 10 or not odno.isascii() or not odno.isdigit() or not int(odno):
        return None
    matches = [row for row in candidates if str(row.get("broker_order_id")) == odno
               and str(row.get("trade_date")) == day.isoformat()]
    if len(matches) != 1:
        return None
    intent = matches[0]
    try:
        raw = order.get("response_json")
        row = json.loads(raw) if isinstance(raw, str) else raw
        q = int(order["qty"])
        amount = Decimal(str(row["tot_ccld_amt"]))
        avg = Decimal(str(row["avg_prvs"]))
        filled_amount = Decimal(str(intent["filled_notional_krw"]))
        if (q <= 0 or not amount.is_finite() or amount <= 0 or
                not avg.is_finite() or avg <= 0 or not filled_amount.is_finite()
                or row.get("source_order_id") != str(order["order_id"])
                or row.get("source_client_order_key") != order.get("client_order_key")
                or order.get("client_order_key") !=
                    f"practice:pb1_pullback_close:{day:%Y%m%d}:122630:SELL:{odno}"
                or order.get("broker_order_id") != odno
                or row.get("pdno") != "122630" or row.get("odno") != odno
                or row.get("ord_dt") != day.strftime("%Y%m%d")
                or row.get("sll_buy_dvsn_cd") != "01" or row.get("cncl_yn") != "N"
                or int(row["rmn_qty"]) != 0 or int(row["ord_qty"]) != q
                or int(row["tot_ccld_qty"]) != q or int(row["rjct_qty"]) != 0
                or int(row["cncl_cfrm_qty"]) != 0
                or amount != filled_amount or abs(amount / q - avg) > Decimal("1")
                or intent.get("status") != "FILLED"
                or intent.get("strategy_id") != "KR_INFINITE_V1"
                or intent.get("strategy_owner") != "KR_INFINITE"
                or intent.get("account_bound") is not True
                or intent.get("action_state") != "SATISFIED"
                or int(intent.get("claim_conflicts", -1)) != 0
                or intent.get("active_attempt_id") is not None
                or intent.get("attempt_state") != "FILLED"
                or intent.get("authoritative") is not True
                or intent.get("attempt_bound") is not True
                or intent.get("side") not in {"SELL_PARTIAL", "SELL_ALL"}
                or intent.get("action") not in {"SELL:TP1", "SELL:TP2"}
                or not intent.get("cycle_id")
                or any(int(intent[k]) != q for k in (
                    "requested_qty", "filled_qty", "target_qty",
                    "cumulative_filled_qty", "attempt_requested_qty",
                    "attempt_filled_qty"))
                or int(intent["remaining_target_qty"]) != 0):
            return None
    except (KeyError, TypeError, ValueError, InvalidOperation, AttributeError):
        return None
    return {"order_id": str(order["order_id"]), "broker_order_id": odno,
            "trade_date": day.isoformat(), "cycle_id": str(intent["cycle_id"]),
            "owner": "KR_INFINITE", "qty": q, "notional": str(amount),
            "proof_source": "STORED_KIS_CUMULATIVE_AND_OWNER_CLAIM_ATTEMPT",
            "fresh_kis_query": False}


def audit_sell_settlement(
    *, orders: list[dict], fills: list[dict], positions: list[dict],
    trade_date: date | None = None,
    infinite_owner_settlements: list[dict] | None = None,
) -> dict[str, Any]:
    """Return missing priced SELL fill quantities, never synthesize executions."""
    by_order: dict[str, list[dict]] = defaultdict(list)
    for fill in fills:
        if str(fill.get("side") or "").upper() == "SELL":
            by_order[str(fill.get("order_id") or "")].append(fill)
    pos_by_cycle = {
        str(row.get("position_cycle_id") or ""): row for row in positions
        if row.get("position_cycle_id")
    }
    gaps = []
    inspected = 0
    excluded_infinite_mirror_orders = 0
    stored_infinite_proofs = []
    explicit_preack_rejections_excluded = 0
    bucket_counts = {"today": 0, "historical": 0, "undated": 0}
    certified_mirror_sell_ids = set()
    for position in positions:
        if (
            str(position.get("code") or "") == "122630"
            and str(position.get("status") or "").upper() == "CLOSED"
            and str(position.get("closed_reason") or "").upper()
            == "KR_INFINITE_LEGACY_MIRROR_RETIRED"
        ):
            raw = position.get("position_meta") or {}
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (TypeError, ValueError):
                    raw = {}
            proof = (raw.get("kr_infinite_legacy_mirror_retired") or {}) if isinstance(raw, dict) else {}
            if (
                isinstance(proof, dict)
                and proof.get("result") == "VERIFIED_FULL_TP2_EXIT"
                and proof.get("source") == "ORIGINAL_KIS_FULL_ORDER_CCLD_AND_EXECUTION_CLAIMS"
            ):
                certified_mirror_sell_ids.update(
                    str(x) for x in proof.get("verified_sell_broker_order_ids", [])
                    if isinstance(x, str) and len(x) == 10 and x.isdigit()
                )
    for order in orders:
        if str(order.get("side") or "").upper() != "SELL":
            continue
        if _explicit_kr_preack_rejection_not_a_broker_order(
            order, by_order.get(str(order.get("order_id") or ""), []),
        ):
            explicit_preack_rejections_excluded += 1
            continue
        if (
            str(order.get("code") or "") == "122630"
            and str(order.get("status") or "").upper() == "FILLED"
            and str(order.get("kis_odno") or "") in certified_mirror_sell_ids
        ):
            # Not a PB1 economic SELL: the KR_INFINITE owner has separately
            # certified these exact broker IDs and retired its legacy mirror.
            excluded_infinite_mirror_orders += 1
            continue
        if not by_order.get(str(order.get("order_id") or "")):
            owner_proof = _stored_infinite_sell_proof(order, infinite_owner_settlements or [])
            if owner_proof is not None:
                stored_infinite_proofs.append(owner_proof)
                continue
        inspected += 1
        dated = _broker_order_day(order.get("created_at"))
        bucket = (
            "undated" if dated is None
            else "today" if dated == trade_date
            else "historical" if dated < trade_date
            else "undated"
        ) if trade_date is not None else "undated"
        bucket_counts[bucket] += 1
        order_id = str(order.get("order_id") or "")
        requested = _number(order.get("qty"))
        broker_nonexecution = _broker_confirmed_nonexecution(order, requested)
        # Only the KIS-confirmed unexecuted part is exempt from priced fills.
        # A cancelled/rejected DB status alone can never hide executed shares.
        required_priced_qty = requested - broker_nonexecution
        rows = by_order.get(order_id, [])
        # Per-order KIS cumulative snapshots must never be summed. Individual
        # trade executions can be summed only in their own evidence lane.
        cumulative = [
            _number(row.get("qty")) for row in rows
            if str(row.get("evidence_kind") or "").upper() == "KIS_ORDER_CUMULATIVE_EXECUTION"
            and float(row.get("price") or 0) > 0
        ]
        individual = [
            _number(row.get("qty")) for row in rows
            if str(row.get("evidence_kind") or "").upper() == "KIS_INDIVIDUAL_EXECUTION"
            and float(row.get("price") or 0) > 0
        ]
        # Manual/unknown-price DB entries are not native KIS economic proof.
        # Only native cumulative snapshots or individual KIS executions count.
        durable_qty = max(max(cumulative, default=0), sum(individual))
        position = pos_by_cycle.get(str(order.get("position_cycle_id") or ""), {})
        closed = str(position.get("status") or "").upper() == "CLOSED"
        pending_reason = str(position.get("closed_reason") or "").upper() == "BROKER_ZERO_ECONOMIC_SETTLEMENT_PENDING"
        missing_qty = max(0, required_priced_qty - durable_qty)
        # An order status, even FILLED, cannot substitute for native priced fills.
        if missing_qty or pending_reason:
            gaps.append({
                "name": order.get("name") or order.get("code"),
                "code": order.get("code"),
                "order_id": order_id,
                "kis_odno": order.get("kis_odno"),
                "order_status": order.get("status"),
                "trade_day_bucket": bucket,
                "source_order_day": dated.isoformat() if dated else None,
                "requested_sell_qty": requested,
                "broker_confirmed_nonexecution_qty": broker_nonexecution,
                "required_priced_sell_qty": required_priced_qty,
                "durable_priced_sell_qty": durable_qty,
                "missing_priced_sell_qty": missing_qty,
                "db_position_closed": closed,
                "db_economic_pending": pending_reason,
                "reason": "MISSING_NATIVE_KIS_PRICE_OR_QTY" if missing_qty else "MANUAL_SETTLEMENT_NOT_PROVEN",
            })
    per_bucket_gaps = {
        bucket: sum(gap["trade_day_bucket"] == bucket for gap in gaps)
        for bucket in bucket_counts
    }
    return {
        "status": "ECONOMIC_SETTLEMENT_PENDING" if gaps else "PRICED_SELL_EVIDENCE_MATCHED",
        "trade_date": trade_date.isoformat() if trade_date else None,
        "today_sell_orders_checked": bucket_counts["today"],
        "historical_sell_orders_checked": bucket_counts["historical"],
        "undated_sell_orders_checked": bucket_counts["undated"],
        "today_gap_count": per_bucket_gaps["today"],
        "historical_gap_count": per_bucket_gaps["historical"],
        "undated_gap_count": per_bucket_gaps["undated"],
        "today_settlement_status": (
            "UNVERIFIED_TRADE_DATE" if trade_date is None or bucket_counts["undated"]
            else "ECONOMIC_SETTLEMENT_PENDING" if per_bucket_gaps["today"]
            else "TODAY_PRICED_SELL_EVIDENCE_MATCHED"
        ),
        "historical_settlement_status": (
            "HISTORICAL_ECONOMIC_PENDING" if per_bucket_gaps["historical"] or per_bucket_gaps["undated"]
            else "HISTORICAL_PRICED_SELL_EVIDENCE_MATCHED"
        ),
        "sell_orders_checked": inspected,
        "certified_infinite_mirror_sells_excluded": excluded_infinite_mirror_orders,
        "stored_infinite_owner_sells_verified": len(stored_infinite_proofs),
        "stored_infinite_owner_evidence": stored_infinite_proofs,
        "explicit_preack_rejections_excluded": explicit_preack_rejections_excluded,
        "gap_count": len(gaps),
        "gaps": gaps,
        "note": "Read-only DB evidence. Broker-native replay requires separate KIS read-only proof.",
    }


def load_snapshot(engine: sa.Engine, *, env: str, epoch: str, day: date) -> dict[str, list[dict]]:
    if not epoch or not day:
        raise ValueError("Trading epoch and local trade date are mandatory")
    from sqlalchemy import text
    # All SQL is SELECT-only. The exact trading epoch isolates the account;
    # the date is an *as-of* cutoff, not a lower bound. Previous-day SELL gaps
    # must remain visible until native economic settlement has been proven.
    # The old SQLite read-only audit fixtures intentionally lack the
    # post-ACK identity columns; never infer a KIS denial in that case.
    extra_projection = (
        "NULL AS client_order_key, NULL AS broker_order_id, "
        "NULL AS acked_at, NULL AS request_json"
        if engine.dialect.name == "sqlite"
        else "client_order_key, broker_order_id, acked_at, request_json"
    )
    with engine.connect() as conn:
        orders = [dict(x) for x in conn.execute(text(f"""
            SELECT order_id, code, side, status, qty, kis_odno,
                   position_cycle_id, {extra_projection}, response_json, created_at
            FROM public.orders
            WHERE env=:env AND trading_epoch_id=:epoch AND side='SELL'
              AND created_at < :end_utc
        """), {
            "env": env, "epoch": epoch,
            "end_utc": __import__("datetime").datetime.combine(day + timedelta(days=1), __import__("datetime").time.min, tzinfo=__import__("datetime").timezone(__import__("datetime").timedelta(hours=9))).astimezone(__import__("datetime").timezone.utc),
        }).mappings()]
        fills = [dict(x) for x in conn.execute(text("""
            SELECT order_id, side, qty, price,
                   fill_meta_json ->> 'evidence_kind' AS evidence_kind
            FROM public.fills WHERE env=:env AND trading_epoch_id=:epoch AND side='SELL'
              AND filled_at < :end_utc
        """), {
            "env": env, "epoch": epoch,
            "end_utc": __import__("datetime").datetime.combine(day + timedelta(days=1), __import__("datetime").time.min, tzinfo=__import__("datetime").timezone(__import__("datetime").timedelta(hours=9))).astimezone(__import__("datetime").timezone.utc),
        }).mappings()]
        # Production PostgreSQL has position_meta JSONB. Legacy SQLite
        # compatibility fixtures intentionally omit the column; they must
        # continue auditing original missing-price SELLs normally, but can
        # never supply a certified Infinite mirror exemption.
        meta_projection = (
            "NULL AS position_meta" if engine.dialect.name == "sqlite"
            else "position_meta"
        )
        positions = [dict(x) for x in conn.execute(text(f"""
            SELECT code, position_cycle_id, status, qty, closed_reason, {meta_projection}
            FROM public.positions WHERE env=:env AND trading_epoch_id=:epoch
        """), {"env": env, "epoch": epoch}).mappings()]
        infinite_owner_settlements = []
        if env == "practice" and engine.dialect.name == "postgresql":
            # Exact account+epoch, dedicated intent, terminal claim, unique
            # authoritative attempt. No dependence on the newer current cycle.
            infinite_owner_settlements = [dict(row) for row in conn.execute(text("""
                SELECT i.strategy_id, i.cycle_id, i.side, i.status, i.trade_date,
                    i.broker_order_id, i.requested_qty, i.filled_qty,
                    i.filled_notional_krw, c.strategy_owner, c.action_state,
                    c.action, c.target_qty, c.cumulative_filled_qty,
                    c.remaining_target_qty, c.claim_conflicts, c.active_attempt_id,
                    a.attempt_state, a.authoritative,
                    a.requested_qty AS attempt_requested_qty,
                    a.cumulative_filled_qty AS attempt_filled_qty,
                    (c.account_id=encode(sha256(convert_to(t.account_id,'UTF8')),'hex')) AS account_bound,
                    (a.attempt_id=i.metadata::jsonb->>'execution_submit_attempt_id'
                     AND a.client_order_key=i.idempotency_key) AS attempt_bound
                FROM public.kr_infinite_order_intents i
                JOIN public.trading_epochs t ON t.trading_epoch_id=i.trading_epoch_id
                    AND t.env=:env AND t.status='ACTIVE'
                JOIN public.execution_claims c ON c.action_key=i.metadata::jsonb->>'execution_claim_action_key'
                    AND c.env=t.env AND c.market='KR' AND c.trading_epoch_id=t.trading_epoch_id
                    AND c.trade_date=i.trade_date AND c.lifecycle_id='122630:'||i.cycle_id
                JOIN public.execution_attempts a ON a.action_key=c.action_key
                WHERE i.trading_epoch_id=:epoch AND i.symbol='122630'
                    AND i.strategy_id='KR_INFINITE_V1'
                    AND i.side IN ('SELL_PARTIAL','SELL_ALL') AND i.trade_date<=:day
                    AND i.metadata::jsonb->>'strategy_owner'='KR_INFINITE'
                    AND i.metadata::jsonb->>'trading_epoch_id'=i.trading_epoch_id
                    AND (SELECT count(*) FROM public.execution_attempts z
                        WHERE z.action_key=c.action_key)=1
            """), {"epoch": epoch, "env": env, "day": day}).mappings()]
    # Use the broker's own product name when present, but never trust a JSON
    # label as strategy owner or settlement authority.
    for row in orders:
        try:
            raw = json.loads(row.get("response_json") or "{}")
            row["name"] = raw.get("prdt_name") or row.get("code")
        except (ValueError, TypeError):
            row["name"] = row.get("code")
    return {"orders": orders, "fills": fills, "positions": positions,
            "infinite_owner_settlements": infinite_owner_settlements}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KR practice close SELL economic audit (read only)")
    parser.add_argument("--epoch", required=True, help="Exact trading_epoch_id of active KIS practice account")
    parser.add_argument("--trade-date", required=True, help="KR date YYYY-MM-DD")
    parser.add_argument("--env", default="practice", choices=["practice", "real"])
    args = parser.parse_args(argv)
    from trader.db.engine import get_engine
    engine = get_engine()
    report = audit_sell_settlement(**load_snapshot(
        engine, env=args.env, epoch=args.epoch, day=date.fromisoformat(args.trade_date),
    ), trade_date=date.fromisoformat(args.trade_date))
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0 if report["gap_count"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
