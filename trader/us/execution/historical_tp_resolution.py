"""Resolve intent-only historical TP fences from original-day broker evidence.

No synthetic execution attempt, order, fill or position cycle is created.
A blank broker response never authorizes release. The historical day must be
positively witnessed and the authenticated all-status query complete. Every
target SELL must be absent or exactly attributed to a completed prior TP stage.
Native/ambiguous evidence wins.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
import logging
import math
import re
import time
from uuid import uuid4

from sqlalchemy import text
from trader.us.utils.order_no import normalize_us_order_no

logger = logging.getLogger(__name__)
EVIDENCE = "KIS_ORIGINAL_DAY_NO_ACCEPTED_SELL"
PRIOR_STAGE_EVIDENCE = "KIS_ORIGINAL_DAY_ONLY_SETTLED_PRIOR_TP_SELLS"
EARLY_FAILURES = {"US_RECOVERY_SCOPE_UNVERIFIED", "ORDER_DISABLED_DURABLE_LEDGER_UNAVAILABLE"}
CANDIDATES_SQL = """
 SELECT * FROM us_profit_capture_lifecycle
 WHERE trading_epoch_id=:epoch AND trade_date<:today
   AND stage_status IN ('NOT_TRIGGERED','PENDING')
   AND cumulative_filled_qty=0 AND symbol<>'TQQQ'
   AND COALESCE(canonical_broker_order_no,'')=''
   AND COALESCE(raw_broker_order_no,'')=''
   AND client_order_key IS NOT NULL AND client_order_key<>''
   AND COALESCE(state->'meta'->>'last_status',state->>'last_status','')
       IN ('US_RECOVERY_SCOPE_UNVERIFIED','ORDER_DISABLED_DURABLE_LEDGER_UNAVAILABLE')
 ORDER BY trade_date,symbol,stage
"""
WITNESSES_SQL = """
 SELECT DISTINCT o.trade_date,o.symbol,o.side,o.order_no,o.qty_requested,o.qty_filled,o.account_id
 FROM us_orders o JOIN us_fills f
   ON f.trading_epoch_id=o.trading_epoch_id AND f.trade_date=o.trade_date
  AND f.symbol=o.symbol AND f.side=o.side
  AND ltrim(f.order_no,'0')=ltrim(o.order_no,'0')
 WHERE o.trading_epoch_id=:epoch AND o.trade_date=:day
   AND o.qty_filled>0 AND f.qty>0 AND o.order_no IS NOT NULL
   AND COALESCE(f.meta->>'is_synthetic','false')='false'
   AND f.meta->>'fill_evidence_type' IN ('KIS_ORDER_CUMULATIVE_ACTUAL','KIS_EXECUTION_ACTUAL')
"""
SETTLED_PRIOR_SQL = """
 SELECT DISTINCT o.trade_date,o.symbol,o.side,o.order_no,o.qty_requested,o.qty_filled,
        o.account_id,o.client_order_key,o.meta->>'strategy_owner' AS strategy_owner,
        s.stage,s.stage_status,s.position_lifecycle_id,s.cumulative_filled_qty,
        s.raw_broker_order_no,s.canonical_broker_order_no,s.trading_epoch_id
 FROM us_orders o JOIN us_profit_capture_lifecycle s
   ON s.client_order_key=o.client_order_key AND s.trading_epoch_id=o.trading_epoch_id
  AND s.trade_date=o.trade_date AND s.symbol=o.symbol
 WHERE o.trading_epoch_id=:epoch AND o.trade_date=:day AND o.symbol=:symbol
   AND o.side='SELL' AND o.status='FILLED' AND s.stage_status='DONE'
   AND EXISTS (SELECT 1 FROM us_fills f WHERE f.trading_epoch_id=o.trading_epoch_id
       AND f.trade_date=o.trade_date AND f.symbol=o.symbol AND f.side=o.side
       AND f.client_order_key=o.client_order_key AND ltrim(f.order_no,'0')=ltrim(o.order_no,'0')
       AND f.qty>0 AND COALESCE(f.meta->>'is_synthetic','false')='false'
       AND f.meta->>'fill_evidence_type' IN ('KIS_ORDER_CUMULATIVE_ACTUAL','KIS_EXECUTION_ACTUAL'))
   AND NOT EXISTS (SELECT 1 FROM us_orders other WHERE other.trading_epoch_id=o.trading_epoch_id
       AND other.trade_date=o.trade_date AND ltrim(other.order_no,'0')=ltrim(o.order_no,'0')
       AND other.client_order_key<>o.client_order_key)
"""


def _settled_prior_match(original: dict, broker: dict, known: dict, scope: dict) -> bool:
    """Attribute by native order identity and completed prior stage, never qty alone."""
    stage = str(known.get('stage') or '')
    if stage not in {'tp1','tp2'} or original['stage'] not in {'tp2','tp3'} or stage >= original['stage']:
        return False
    prefix = (f"US_PC_{original['trade_date']}_{original['symbol']}_"
              f"{original['position_lifecycle_id']}_TAKE_PROFIT_{stage.upper()}")
    qty = number(broker.get('requested_qty'))
    order = normalize_us_order_no(broker.get('order_no'))
    return bool(qty and broker.get('status') == 'FILLED' and number(broker.get('filled_qty')) == qty
        and known.get('stage_status') == 'DONE' and known.get('side') == 'SELL'
        and str(known.get('trade_date')) == str(original['trade_date'])
        and known.get('symbol') == original['symbol']
        and known.get('trading_epoch_id') == scope['trading_epoch_id']
        and known.get('position_lifecycle_id') == original['position_lifecycle_id']
        and known.get('strategy_owner') in (None, '', 'US_STANDARD')
        and known.get('account_id') in (None, '', scope['account_id'], scope.get('raw_account'))
        and re.fullmatch(re.escape(prefix)+r'(?:_R[1-9][0-9]*)?', str(known.get('client_order_key') or ''))
        and known.get('client_order_key') != original['client_order_key']
        and number(known.get('qty_requested')) == qty and number(known.get('qty_filled')) == qty
        and number(known.get('cumulative_filled_qty')) == qty
        and all(normalize_us_order_no(known.get(k)) == order for k in
                ('order_no','raw_broker_order_no','canonical_broker_order_no')))


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), default=str).encode()).hexdigest()


def number(value):
    if isinstance(value, bool): return None
    try:
        n = float(value)
        return int(n) if math.isfinite(n) and n >= 0 and n.is_integer() else None
    except (ValueError, TypeError, OverflowError):
        return None


def metadata(row: dict) -> dict:
    state = row.get("state") or {}
    if isinstance(state, str): state = json.loads(state)
    if not isinstance(state, dict): return {}
    layers, inner = [], state
    for _ in range(32):
        if not isinstance(inner, dict): break
        layers.append({k: v for k, v in inner.items() if k != "meta"})
        inner = inner.get("meta")
    flat = {}
    # New outer fields win over stale recursively copied historical state.
    for layer in reversed(layers): flat.update(layer)
    return flat


def eligible(row: dict) -> bool:
    meta = metadata(row)
    symbol, stage = str(row.get("symbol") or ""), str(row.get("stage") or "")
    cycle, key = str(row.get("position_lifecycle_id") or ""), str(row.get("client_order_key") or "")
    return bool(symbol and symbol != "TQQQ" and cycle and stage in {"tp1", "tp2", "tp3"}
        and row.get("stage_status") in {"NOT_TRIGGERED", "PENDING"}
        and number(row.get("cumulative_filled_qty")) == 0
        and not row.get("raw_broker_order_no") and not row.get("canonical_broker_order_no")
        and meta.get("last_status") in EARLY_FAILURES
        and meta.get("last_stage") == stage
        and meta.get("strategy_owner", "US_STANDARD") == "US_STANDARD"
        and key.startswith(f"US_PC_{row.get('trade_date')}_{symbol}_{cycle}_TAKE_PROFIT_{stage.upper()}"))


def assess(row: dict, snapshot: dict, witnesses: list[dict], scope: dict,
           *, now: datetime | None = None, settled_prior: list[dict] | None = None) -> dict:
    """A positive day witness is required even for a fully paginated empty book."""
    reject = lambda reason: {"verified": False, "reason": reason}
    if not eligible(row) or str(row.get("trading_epoch_id") or "") != scope.get("trading_epoch_id"):
        return reject("LOCAL_TP_SOURCE_NOT_ELIGIBLE")
    day, symbol = str(row["trade_date"]), row["symbol"]
    if snapshot.get("status") != "OK": return reject("ORIGINAL_DAY_QUERY_UNAVAILABLE")
    bs, query = snapshot.get("scope") or {}, snapshot.get("query") or {}
    if not (bs.get("verified") is True and bs.get("source") == "kis_authenticated_order_book"
            and bs.get("env") == scope.get("env") == "practice"
            and bs.get("market") == "US" and bs.get("account_id") == scope.get("account_id")
            and bs.get("query_trade_date") == day):
        return reject("ORIGINAL_DAY_ACCOUNT_SCOPE_UNVERIFIED")
    if not (query.get("complete") is True and query.get("us_trade_date") == day
            and query.get("query_id") and int(query.get("pages") or 0)>0
            and all(query.get(k) is True for k in ("all_symbols", "all_sides", "all_statuses", "all_exchanges"))):
        return reject("ORIGINAL_DAY_QUERY_NOT_COMPLETE")
    try:
        at = datetime.fromisoformat(str(snapshot["observed_at"]).replace("Z", "+00:00"))
        age = ((now or datetime.now(timezone.utc)) - at).total_seconds()
        if at.tzinfo is None or not 0 <= age <= 120: return reject("ORIGINAL_DAY_SNAPSHOT_STALE")
    except (ValueError, TypeError, KeyError):
        return reject("ORIGINAL_DAY_SNAPSHOT_STALE")
    rows = snapshot.get("rows")
    if not isinstance(rows, list) or snapshot.get("rows_sha256") != digest(rows):
        return reject("ORIGINAL_DAY_SNAPSHOT_INTEGRITY_INVALID")
    if not rows: return reject("EMPTY_HISTORY_IS_NOT_ABSENCE_PROOF")
    target_sells, prior_orders = [], []
    for r in rows:
        if (not isinstance(r, dict) or r.get("normalization_result") != "normalized"
                or str(r.get("trade_date") or "").replace("-", "") != day.replace("-", "")
                or not r.get("symbol") or r.get("side") not in {"BUY", "SELL"}
                or not normalize_us_order_no(r.get("order_no"))
                or r.get("requested_qty_present") is not True
                or r.get("filled_qty_present") is not True
                or number(r.get("requested_qty")) is None or number(r.get("filled_qty")) is None
                or number(r.get("requested_qty")) <= 0
                or number(r.get("filled_qty")) > number(r.get("requested_qty"))):
            return reject("ORIGINAL_DAY_ROW_SCOPE_OR_QUANTITY_UNVERIFIED")
        # Never identify an unknown historical TP by symbol+quantity alone.
        if str(r["symbol"]).upper() == symbol and r["side"] == "SELL":
            matches = [k for k in (settled_prior or []) if _settled_prior_match(row, r, k, scope)]
            if len(matches) == 1:
                prior_orders.append(digest({'order_no': normalize_us_order_no(r['order_no']),
                    'key': matches[0]['client_order_key'], 'stage': matches[0]['stage']}))
                continue
            target_sells.append({"broker_order_sha256": digest(normalize_us_order_no(r["order_no"])),
                "status": r.get("status"), "requested_qty": r["requested_qty"],
                "filled_qty": r["filled_qty"], "remaining_qty": r.get("remaining_qty")})
    if target_sells:
        return {**reject("BROKER_SELL_PRESENT_REQUIRES_ORIGINAL_ATTRIBUTION"),
                "broker_sell_observations": target_sells}
    witness_hash = None
    for w in witnesses:
        if w.get("account_id") not in (None, "", scope["account_id"], scope.get("raw_account")):
            continue
        for r in rows:
            if (str(w.get("trade_date")) == day and w.get("symbol") == r["symbol"]
                    and w.get("side") == r["side"]
                    and normalize_us_order_no(w.get("order_no")) == normalize_us_order_no(r.get("order_no"))
                    and number(w.get("qty_requested")) == number(r.get("requested_qty"))
                    and number(w.get("qty_filled")) is not None and number(w["qty_filled"]) > 0
                    and number(r["filled_qty"]) >= number(w["qty_filled"])):
                witness_hash = digest({"order_no": normalize_us_order_no(w["order_no"]),
                                       "symbol": w["symbol"], "side": w["side"], "trade_date": day})
                break
    if not witness_hash: return reject("HISTORICAL_DAY_NOT_POSITIVELY_WITNESSED")
    return {"verified": True, "reason": PRIOR_STAGE_EVIDENCE if prior_orders else EVIDENCE, "source_trade_date": day,
            "symbol": symbol, "stage": row["stage"], "source_cycle": row["position_lifecycle_id"],
            "source_key_sha256": digest(row["client_order_key"]), "account_id": scope["account_id"],
            "trading_epoch_id": scope["trading_epoch_id"], "query_id": query["query_id"],
            "observed_at": snapshot["observed_at"], "snapshot_sha256": snapshot["rows_sha256"],
            "witness_sha256": witness_hash, "pages": query["pages"],
            "settled_prior_order_sha256": prior_orders,
            "meaning": "NO_UNATTRIBUTED_SELL_IN_VERIFIED_ORIGINAL_DAY_NOT_A_PRE_HTTP_CERTIFICATE"}


def _scope(conn) -> dict:
    from trader.us.db.repos import _us_execution_claim_scope, _active_us_epoch
    from trader.account_state import get_account_key
    scope = _us_execution_claim_scope()
    raw = get_account_key(env="practice")
    if scope.get("env") != "practice" or _active_us_epoch(conn) != scope.get("trading_epoch_id"):
        raise ValueError("ACTIVE_TP_ACCOUNT_EPOCH_UNVERIFIED")
    epoch = conn.execute(text("""SELECT env,status,account_id FROM trading_epochs
        WHERE trading_epoch_id=:epoch"""), {"epoch": scope["trading_epoch_id"]}).mappings().one_or_none()
    if (not epoch or epoch.get("env") != "practice" or epoch.get("status") != "ACTIVE"
            or epoch.get("account_id") not in {raw, scope["account_id"]}):
        raise ValueError("ACTIVE_TP_ACCOUNT_EPOCH_UNVERIFIED")
    return {**scope, "raw_account": raw}


def _no_local_broker_evidence(conn, row: dict, scope: dict) -> bool:
    p = {"key": row["client_order_key"], "epoch": scope["trading_epoch_id"],
         "symbol": row["symbol"], "cycle": row["position_lifecycle_id"], "account": scope["account_id"],
         "pattern": f"US_PC_%_{row['symbol']}_%"}
    for table in ("us_orders", "us_fills", "us_execution_attempts", "us_order_events"):
        if conn.execute(text(f"SELECT 1 FROM {table} WHERE client_order_key=:key LIMIT 1"), p).first():
            return False
    conflict = conn.execute(text("""
        SELECT 1 FROM us_execution_claims c
        JOIN us_execution_attempts a ON a.attempt_id=c.active_attempt_id
        LEFT JOIN us_order_intents i ON i.client_order_key=a.client_order_key
        LEFT JOIN us_orders o ON o.client_order_key=a.client_order_key
        WHERE c.env='practice' AND c.market='US' AND c.account_id=:account
          AND c.trading_epoch_id=:epoch AND c.active_attempt_id IS NOT NULL
          AND (c.lifecycle_id=:cycle OR i.symbol=:symbol OR o.symbol=:symbol
               OR i.meta->>'source_original_runtime_lifecycle_id'=:cycle
               OR a.client_order_key LIKE :pattern)
        LIMIT 1
    """), p).first()
    if conflict: return False
    return not conn.execute(text("""SELECT 1 FROM us_orders
        WHERE trading_epoch_id=:epoch AND symbol=:symbol AND side='SELL'
          AND UPPER(status) NOT IN ('FILLED','CANCELLED','CANCELED','REJECTED','EXPIRED') LIMIT 1"""), p).first()


def _intent_valid(conn, row: dict, scope: dict, *, lock: bool = False) -> bool:
    p = {"day": str(row["trade_date"]), "symbol": row["symbol"], "stage": row["stage"],
         "cycle": row["position_lifecycle_id"], "key": row["client_order_key"], "epoch": scope["trading_epoch_id"]}
    sql = "SELECT * FROM us_order_intents WHERE client_order_key=:key" + (" FOR UPDATE" if lock else "")
    intent = conn.execute(text(sql), p).mappings().one_or_none()
    if intent:
        im = intent.get("meta") or {}
        from trader.us.profit_capture import tp_stage_lifecycle_from_order_proof
        life = str(im.get("position_lifecycle_id") or "")
        if life:
            try: life = tp_stage_lifecycle_from_order_proof(life, im, client_order_key=p["key"])
            except ValueError: return False
        if (str(intent["trade_date"]) != p["day"] or intent["symbol"] != p["symbol"]
                or intent["side"] != "SELL" or intent["trading_epoch_id"] != p["epoch"]
                or intent["status"] not in {"PENDING", "BLOCKED"}
                or (life and life != p["cycle"])
                or intent.get("account_id") not in (None, "", scope["account_id"], scope["raw_account"])
                or im.get("strategy_owner", "US_STANDARD") != "US_STANDARD"):
            return False
    return True


def _apply(conn, original: dict, snapshot: dict, scope: dict) -> tuple[bool, str]:
    from trader.us.db.repos import mark_us_profit_capture_stage, append_us_order_event, load_us_profit_capture_state
    p = {"day": str(original["trade_date"]), "symbol": original["symbol"], "stage": original["stage"],
         "cycle": original["position_lifecycle_id"], "key": original["client_order_key"],
         "epoch": scope["trading_epoch_id"]}
    row = conn.execute(text("""SELECT * FROM us_profit_capture_lifecycle
        WHERE trade_date=:day AND symbol=:symbol AND stage=:stage
          AND position_lifecycle_id=:cycle AND trading_epoch_id=:epoch FOR UPDATE"""), p).mappings().one_or_none()
    if not row or digest(dict(row)) != digest(original): return False, "SOURCE_CHANGED_DURING_BROKER_QUERY"
    # Re-read ACTIVE scope and witness evidence under the same transaction.
    current_scope = _scope(conn)
    if current_scope != scope: return False, "ACTIVE_SCOPE_CHANGED_DURING_BROKER_QUERY"
    witnesses = [dict(r) for r in conn.execute(text(WITNESSES_SQL), p).mappings().all()]
    prior = [dict(r) for r in conn.execute(text(SETTLED_PRIOR_SQL), p).mappings().all()]
    proof = assess(dict(row), snapshot, witnesses, scope, settled_prior=prior)
    if not proof["verified"]: return False, proof["reason"]
    if not _no_local_broker_evidence(conn, dict(row), scope): return False, "LOCAL_BROKER_OR_CLAIM_EVIDENCE_PRESENT"
    if not _intent_valid(conn, dict(row), scope, lock=True):
        return False, "ORIGINAL_INTENT_SCOPE_UNVERIFIED"
    patch = {"historical_tp_resolution": proof}
    conn.execute(text("""UPDATE us_order_intents SET status='BLOCKED',
        meta=COALESCE(meta,'{}'::jsonb)||CAST(:patch AS jsonb)
        WHERE client_order_key=:key AND trading_epoch_id=:epoch AND status='PENDING'"""),
        {**p, "patch": json.dumps(patch)})
    mark_us_profit_capture_stage(p["day"], p["symbol"], p["stage"], order_key=p["key"],
        position_lifecycle_id=p["cycle"], status="FAILED", evidence_type=proof['reason'], _conn=conn)
    event = {"event_id": str(uuid4()), "trade_date": p["day"],
        "event_type": "TP_HISTORICAL_BROKER_ABSENCE_VERIFIED", "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "session": "BOUNDARY_RECONCILIATION", "session_run_id": None, "tick_id": None,
        "client_order_key": p["key"], "submit_attempt_id": "OBSERVATION:" + proof["query_id"],
        "symbol": p["symbol"], "side": "SELL", "requested_qty": row["requested_qty"],
        "raw_broker_order_no": None, "canonical_broker_order_no": None,
        "position_lifecycle_id": p["cycle"], "profit_capture_stage": p["stage"],
        "cumulative_filled_qty": 0, "trading_epoch_id": p["epoch"],
        "payload": {"proof": proof, "observation_only": True, "not_an_execution_attempt": True},
        "idempotency_key": digest({"key": p["key"], "proof": proof})}
    if not append_us_order_event(event, _conn=conn): raise RuntimeError("TP_RESOLUTION_AUDIT_NOT_DURABLE")
    state = load_us_profit_capture_state(p["day"], [p["symbol"]], {p["symbol"]: p["cycle"]}, _conn=conn)[p["symbol"]]
    if (state.get(p["stage"]+"_pending") or state.get(p["stage"]+"_done")
            or state.get("meta", {}).get("last_evidence_type") != proof['reason']):
        raise RuntimeError("TP_RESOLUTION_POSTCONDITION_FAILED")
    return True, proof['reason']


def resolve_historical_tp_at_boundary(*, provider, trade_date: str, phase: str,
                                      apply: bool = True, budget_sec: float = 20,
                                      source_trade_date: str | None = None) -> dict:
    result = {"status": "OK", "checked": 0, "released": 0, "eligible": 0,
              "source_days_queried": 0, "source_day_evidence": [],
              "unresolved": [], "observations": [], "database_mutated": False}
    if str(phase).upper() not in {"AM_START", "AM", "CLOSE"}:
        return {**result, "status": "SKIPPED_NOT_BOUNDARY"}
    from trader.us.db.repos import _get_engine_or_none
    end = time.monotonic() + min(30, max(0.1, float(budget_sec)))
    try:
        engine = _get_engine_or_none()
        if engine is None: raise RuntimeError("DURABLE_TP_DATABASE_UNAVAILABLE")
        with engine.connect() as conn:
            scope = _scope(conn)
            candidates = [dict(r) for r in conn.execute(text(CANDIDATES_SQL),
                {"epoch": scope["trading_epoch_id"], "today": trade_date}).mappings().all()
                if eligible(dict(r)) and (source_trade_date is None or str(r['trade_date']) == source_trade_date)]
        result["candidate_count"] = len(candidates)
        # Stable rotation across AM/Close and days prevents one unavailable
        # old broker day from consuming every boundary's entire read budget.
        if candidates:
            offset = (date.fromisoformat(trade_date).toordinal()*2 + int(str(phase).upper()=="CLOSE")) % len(candidates)
            candidates = candidates[offset:] + candidates[:offset]
        reader = provider.fork_for_stage(stage_deadline=end) if callable(getattr(provider, "fork_for_stage", None)) else provider
        snapshots = {}
        for row in candidates:
            base = {"symbol": row["symbol"], "stage": row["stage"], "source_trade_date": str(row["trade_date"])}
            if time.monotonic() >= end:
                result["unresolved"].append({**base, "reason": "DEFERRED_TO_NEXT_BOUNDARY_BUDGET"}); continue
            result["checked"] += 1
            day = str(row["trade_date"])
            if day not in snapshots:
                try:
                    method = getattr(reader, "get_original_day_order_snapshot", None)
                    snapshots[day] = method(day) if callable(method) else {"status": "UNSUPPORTED"}
                except Exception as exc:
                    snapshots[day] = {"status": "UNAVAILABLE", "error_type": type(exc).__name__}
                result["source_days_queried"] += 1
                snap = snapshots[day]
                query = snap.get("query") or {}
                result["source_day_evidence"].append({
                    "source_trade_date": day, "status": snap.get("status"),
                    "error_type": snap.get("error_type"), "complete": query.get("complete", False),
                    "pages": query.get("pages"), "raw_rows": query.get("raw_rows"),
                    "matched_rows": len(snap.get("rows") or []),
                })
            with engine.connect() as conn:
                witnesses = [dict(r) for r in conn.execute(text(WITNESSES_SQL),
                    {"epoch": scope["trading_epoch_id"], "day": day}).mappings().all()]
                prior = [dict(r) for r in conn.execute(text(SETTLED_PRIOR_SQL),
                    {"epoch": scope["trading_epoch_id"], "day": day, "symbol": row['symbol']}).mappings().all()]
            proof = assess(row, snapshots[day], witnesses, scope, settled_prior=prior)
            if not proof["verified"]:
                detail = {**base, "reason": proof["reason"]}
                if proof.get("broker_sell_observations"):
                    detail["broker_sell_observations"] = proof["broker_sell_observations"]
                result["unresolved"].append(detail); continue
            with engine.connect() as conn:
                live = conn.execute(text("SELECT * FROM us_profit_capture_lifecycle WHERE client_order_key=:key AND trading_epoch_id=:epoch"),
                    {"key": row["client_order_key"], "epoch": scope["trading_epoch_id"]}).mappings().one_or_none()
                valid = bool(live and digest(dict(live)) == digest(row) and _scope(conn) == scope
                             and _no_local_broker_evidence(conn, row, scope) and _intent_valid(conn, row, scope))
            if not valid:
                result["unresolved"].append({**base, "reason": "LOCAL_SOURCE_OR_BROKER_EVIDENCE_CHANGED"}); continue
            result["eligible"] += 1
            if not apply:
                result["observations"].append({**base, "reason": proof["reason"], "apply_required": True,
                    **{k:proof[k] for k in ("query_id","snapshot_sha256","witness_sha256","observed_at")}}); continue
            try:
                with engine.begin() as conn:
                    conn.execute(text("SET LOCAL lock_timeout='1000ms'"))
                    conn.execute(text("SET LOCAL statement_timeout='5000ms'"))
                    ok, reason = _apply(conn, row, snapshots[day], scope)
                if ok:
                    result["released"] += 1; result["database_mutated"] = True
                    result["observations"].append({**base, "reason": reason})
                else: result["unresolved"].append({**base, "reason": reason})
            except Exception as exc:
                result["unresolved"].append({**base, "reason": "ATOMIC_REPAIR_ROLLED_BACK:" + type(exc).__name__})
        if result["unresolved"]: result["status"] = "HISTORICAL_TP_UNRESOLVED"
    except Exception as exc:
        result.update(status="UNAVAILABLE", error_type=type(exc).__name__)
    logger.info("[US_HISTORICAL_TP][BOUNDARY] phase=%s result=%s", phase, result)
    return result
