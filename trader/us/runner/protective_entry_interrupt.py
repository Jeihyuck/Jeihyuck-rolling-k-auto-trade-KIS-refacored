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


_IDENTITY_FIELDS = ("symbol","strategy_owner","position_lifecycle_id","position_cycle_id",
                    "portfolio_epoch_id","trading_epoch_id","broker_account_hash")


def holding_identity(position):
    if not isinstance(position,dict):
        return None
    identity = {k:str(position.get(k) or "").strip() for k in _IDENTITY_FIELDS}
    if (not all(identity.values()) or identity["strategy_owner"]!="US_STANDARD"
            or identity["symbol"].upper()=="TQQQ"
            or identity["position_lifecycle_id"]!=identity["position_cycle_id"]):
        return None
    identity["symbol"]=identity["symbol"].upper()
    meta=position.get("meta")
    if isinstance(meta,dict):
        for key,value in identity.items():
            asserted=str(meta.get(key) or "").strip()
            if key=="symbol":
                asserted=asserted.upper()
            if asserted and asserted!=value:
                return None
    return identity


def _verified_risk_row(position, trade_date, *, fresh_resolver=False):
    """Exact same-day, open account/owner/epoch/portfolio/cycle evidence."""
    from hashlib import sha256
    from trader.us.db.repos import CommittedUSRiskState
    from trader.us.pb1.us_exit_position_resolver import _position_identity,_lifecycle_matches_identity
    try:
        scope=holding_identity(position)
        if scope is None:
            return None
        identity=_position_identity(position,env="practice")
        if (identity["env"]!="practice" or not identity["account_id"]
                or sha256(identity["account_id"].encode()).hexdigest()!=scope["broker_account_hash"]
                or identity["trading_epoch_id"]!=scope["trading_epoch_id"]
                or identity["position_lifecycle_id"]!=scope["position_lifecycle_id"]
                or identity["strategy_owner"]!="US_STANDARD"):
            return None
        row=position.get("_protective_verified_risk_state") if fresh_resolver else position.get("risk_state")
        if not isinstance(row,dict) or (not fresh_resolver and not isinstance(row,CommittedUSRiskState)):
            return None
        state=row.get("state") or {}
        life=state.get("lifecycle") or {}
        if (str(row.get("trade_date") or "")!=str(trade_date)
                or life.get("is_open") is not True
                or str(life.get("portfolio_epoch_id") or "")!=scope["portfolio_epoch_id"]
                or str(life.get("position_cycle_id") or "")!=scope["position_cycle_id"]
                or not _lifecycle_matches_identity(row,life,identity)):
            return None
        return row
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError):
        return None


def _confirmed_soft_snapshot(position, trade_date, *, fresh_resolver=False):
    """Reuse already confirmed state, never manufacture another confirmation."""
    import os
    from trader.us.entry_exit_contract import contract_exit_config
    if os.getenv("US_EARLY_SOFT_STOP_PILOT_ENABLED","0")!="1":
        return None
    try:
        row=_verified_risk_row(position,trade_date,fresh_resolver=fresh_resolver)
        if row is None:
            return None
        scope=holding_identity(position)
        state=row.get("state") or {}
        if position.get("_verified_pb1_stop_revision") and state.get("soft_stop_policy_revision")!="PB1_STOP_3_6_V1":
            return None
        cfg=contract_exit_config(position)
        soft=float(cfg["soft_stop"])
        count=row["soft_stop_breach_count"]
        if (type(count) is not int or not math.isfinite(soft) or not 0<soft<1
                or count<max(1,int(cfg.get("soft_stop_confirm_ticks",2)))
                or not math.isfinite(float(row["last_pnl_pct"]))
                or float(row["last_pnl_pct"]) > -soft):
            return None
        result=deepcopy(position)
        result["risk_state"]=deepcopy(row)
        result["soft_stop_breach_count"]=count
        # Existing repeat/escalation predicates must receive the same current
        # lifecycle's persisted execution fields, never another day's flags.
        execution=state.get("soft_stop_execution") or {}
        if execution and execution.get("position_lifecycle_id")!=scope["position_lifecycle_id"]:
            return None
        for key in ("soft_stop_triggered_today","soft_stop_partial_done","first_soft_stop_at","first_soft_stop_price"):
            result[key]=execution.get(key)
        return result,soft
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError):
        return None


def _verified_candidates(snapshots, excluded_symbols, env, *, trade_date=None, allow_confirmed_soft=False, fresh_resolver=False):
    """Return scoped verified positions and existing frozen stop thresholds."""
    from trader.us.entry_exit_contract import contract_exit_config, us_entry_exit_contract_integrity_state
    from trader.us.pb1.us_exit_router import normalize_book, normalize_horizon
    from trader.us.pb1.us_exit_engine import _resolve_position_entry_price
    from trader.us.symbols import normalize_us_exchange
    if env != "practice":
        return []
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
            if holding_identity(pos) is None:
                continue
            if us_entry_exit_contract_integrity_state(pos, meta) != "VALID":
                continue
            if int(pos["orderable_qty"]) <= 0 or int(pos["qty"]) < int(pos["orderable_qty"]):
                continue
            entry = _resolve_position_entry_price(pos)
            hard = float(contract_exit_config(pos)["hard_stop"])
            if not (math.isfinite(entry) and entry > 0 and math.isfinite(hard) and 0 < hard < 1):
                continue
            soft=None
            if allow_confirmed_soft:
                proof=_confirmed_soft_snapshot(pos,trade_date,fresh_resolver=fresh_resolver)
                if proof is not None and 0<proof[1]<hard:
                    pos,soft=proof
            candidates.append((deepcopy(pos),symbol,normalize_us_exchange(pos["exchange"]),entry,hard,soft))
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
            continue
    return candidates


def make_hard_stop_poll(snapshots, *, excluded_symbols, trade_date, env, context,
                        allow_confirmed_soft=False, fresh_resolver=False):
    """Return a bounded observer; a trigger is a snapshot, never an intent."""
    from trader.us.market_calendar import now_ny, is_us_regular_market_open
    candidates = _verified_candidates(snapshots, excluded_symbols, env,trade_date=trade_date,
        allow_confirmed_soft=allow_confirmed_soft,fresh_resolver=fresh_resolver)
    if not candidates:
        return None
    cursor = 0

    def poll():
        nonlocal cursor
        if not context.broker_submit_allowed() or context.remaining_sec() < 10:
            return None
        now = now_ny()
        if now.date().isoformat() != str(trade_date) or not is_us_regular_market_open(now):
            return None
        # Bounded IPC only. Round robin across all eligible holdings, no REST
        # burst, no concurrent broker authority, and no stale fallback.
        batch,cursor = _bounded_quotes(candidates,cursor,env,
            pair=lambda item:(item[1],item[2]))
        for (pos,symbol,exchange,entry,hard,soft),q in batch:
            try:
                price = _breached_quote(q,entry,hard)
                family="HARD_STOP"
                if price is None and soft is not None:
                    price=_breached_quote(q,entry,soft)
                    family="CONFIRMED_SOFT_STOP"
                if price is None:
                    continue
                last, received = price
                snapshot = deepcopy(pos)
                snapshot["_protective_trigger_family"]=family
                snapshot.update(resolved_current_price=last,current_price_usd=last,
                                current_price=last,resolved_pnl_pct=(last-entry)/entry,
                                resolved_quote_source=q["source"],resolved_quote_asof=received,
                                resolved_quote_received_at=datetime.now(timezone.utc).isoformat(),
                                exit_quote_evidence={"price":last,"source":q["source"],
                                    "asof_epoch":received,"quality":"FRESH",
                                    "collector_generation":q["collector_generation"],
                                    "received_at_utc":datetime.now(timezone.utc).isoformat()})
                if snapshot_still_fresh(snapshot,trade_date,now_ny()):
                    return snapshot
            except (ValueError,TypeError,KeyError,OverflowError):
                continue
        return None
    return poll


def _bounded_quotes(candidates,cursor,env,*,pair):
    """Read up to 48 symbols in bounded batches, without REST or order I/O.

    Advance cursor even on missing replies. No older fallback is substituted.
    Legacy one-symbol reads remain supported.
    """
    from trader.marketdata.us_ws_collector import get_quote,get_quotes_many
    deadline=time.monotonic()+.8
    selected=[]
    remaining_count=min(len(candidates),48)
    while remaining_count:
        remaining=deadline-time.monotonic()
        if remaining<=.01:
            break
        size=min(16,remaining_count)
        chunk=[candidates[(cursor+i)%len(candidates)] for i in range(size)]
        cursor=(cursor+size)%len(candidates)
        remaining_count-=size
        pairs=[pair(item) for item in chunk]
        try:
            if size==1:
                quotes={pairs[0]:get_quote(*pairs[0],env=env,max_age_sec=5,timeout_sec=min(.15,remaining))}
            else:
                quotes=get_quotes_many(pairs,env=env,max_age_sec=5,timeout_sec=min(.2,remaining))
            if not isinstance(quotes,dict):
                quotes={}
        except (OSError,ValueError,TypeError,RuntimeError):
            # Collector failure for one batch is not evidence of a safe
            # holding. Keep the bounded rotation alive for other symbols;
            # never manufacture a quote or retry this IPC in the same poll.
            import logging
            logging.getLogger(__name__).warning("[US_EXIT][PROTECTIVE_IPC_BATCH_UNAVAILABLE] symbols=%d",size)
            quotes={}
        selected.extend((item,quotes.get(p)) for item,p in zip(chunk,pairs))
    return selected,cursor


def _fresh_collector_quote(q):
    if (not isinstance(q,dict) or q.get("source") != "KIS_WEBSOCKET_COLLECTOR"
            or not q.get("collector_generation") or q.get("stale") or q.get("suspect")):
        return None
    last, received = float(q["last"]), float(q["received_at"])
    if not math.isfinite(last) or last<=0 or not 0<=time.time()-received<=5:
        return None
    return last,received


def _breached_quote(q,entry,hard):
    valid=_fresh_collector_quote(q)
    if valid is None or (valid[0]-entry)/entry > -hard:
        return None
    return valid


def profit_watch_levels(position, entry):
    """Advisory only: frozen TP and verified peak price thresholds.

    This cannot authorize a SELL. The child reruns the existing complete
    SELL lane, including stage truth, defense arbitration and portfolio guard.
    """
    from trader.us.entry_exit_contract import (
        extract_us_entry_exit_contract, contract_exit_config, contract_profit_capture,
        US_SWING_V3_CONTRACT_VERSION,
    )
    from trader.us.profit_capture import authoritative_broker_avg
    try:
        reference=float(position.get("resolved_current_price") or 0)
        if not math.isfinite(reference) or reference<=0:
            return None
        up,down=[],[]
        pc=contract_profit_capture(position)
        if pc.get("enabled",True) and pc.get("partial_exit_allowed") is True:
            try:
                avg,_=authoritative_broker_avg(position)
                avg=float(avg)
                for stage in pc.get("stages") or []:
                    flag=str(stage.get("flag") or "")
                    meta=position.get("meta") or {}
                    if flag and not position.get(flag) and not meta.get(flag):
                        threshold=float(stage["threshold_fraction"])
                        if 0<threshold<1:
                            up.append(avg*(1+threshold))
            except (ValueError,TypeError,KeyError,OverflowError):
                pass
        scope=holding_identity(position)
        try:
            high=float(position.get("high_watermark") or 0)
            at=datetime.fromisoformat(str(position.get("high_watermark_at") or "").replace("Z","+00:00"))
            high_ok=(scope and position.get("high_watermark_source")=="us_position_risk_state"
                     and position.get("high_watermark_lifecycle_id")==scope["position_lifecycle_id"]
                     and at.tzinfo is not None and at<=datetime.now(timezone.utc)
                     and math.isfinite(high) and high>entry>0)
        except (ValueError,TypeError,OverflowError):
            # An unavailable peak must not suppress an independent TP hint.
            high_ok=False
        if high_ok:
            contract=extract_us_entry_exit_contract(position,position.get("meta"))
            if contract.get("version")!=US_SWING_V3_CONTRACT_VERSION:
                from trader.us.pb1.us_forward_v3_handoff import verified_forward_v3_management
                approved=verified_forward_v3_management(position)
                if approved:
                    contract=approved[0]
            gain=(high-entry)/entry
            if contract.get("version")==US_SWING_V3_CONTRACT_VERSION:
                policy=contract["management"]["runner_exit"]
                if gain>=float(policy["trailing_activation"]):
                    down.append(high*(1-float(policy["trailing_high_drawdown"])))
                if gain>=float(policy["peak_activation"]):
                    for key in ("partial_giveback","final_trim_giveback","max_giveback"):
                        down.append(high-(high-entry)*float(policy[key]))
            else:
                cfg=contract_exit_config(position)
                if gain>=float(cfg["trailing_activation_profit"]):
                    down.append(high*(1-float(cfg["trailing_stop"])))
                if gain>=float(cfg["profit_protect"]):
                    down.append(high*.97)
                if gain>.05:
                    down.append(high-(high-entry)*float(cfg["giveback"]))
        up=sorted({v for v in up if math.isfinite(v) and v>0})
        down=sorted({v for v in down if math.isfinite(v) and v>0})
        return {"reference":reference,"up":up,"down":down} if up or down else None
    except (ValueError,TypeError,KeyError,AttributeError,OverflowError):
        return None


def profit_threshold_crossed(levels, price, *, previous=None):
    if not isinstance(levels,dict):
        return False
    try:
        before=float(levels["reference"] if previous is None else previous)
        if not all(math.isfinite(v) and v>0 for v in (before,price)):
            return False
        return (any(before<float(v)<=price for v in levels.get("up",[]))
                or any(before>float(v)>=price for v in levels.get("down",[])))
    except (ValueError,TypeError,KeyError,OverflowError):
        return False


def split_wakeup(wakeup):
    """Validate advisory kind without converting it into order authority."""
    if isinstance(wakeup,dict) and set(wakeup)=={"kind","identity"} and wakeup["kind"]=="profit":
        identity=wakeup["identity"]
        if holding_identity(identity)==identity and identity is not None:
            return identity,"profit"
        return None,"invalid"
    if holding_identity(wakeup)==wakeup and wakeup is not None:
        return wakeup,"stop"
    return None,"invalid"


def export_idle_watch(snapshots, *, excluded_symbols, env, trade_date=None, allow_confirmed_soft=False):
    """Private IPC only: advisory identity and thresholds, no contract."""
    rows=[]
    for pos,_,exchange,entry,hard,soft in _verified_candidates(snapshots,excluded_symbols,env,
            trade_date=trade_date,allow_confirmed_soft=allow_confirmed_soft):
        rows.append({"identity":holding_identity(pos),"exchange":exchange,"entry":entry,
                     "hard":hard,"soft":soft,"profit":profit_watch_levels(pos,entry)})
    return rows


def make_idle_hard_stop_poll(watch, *, trade_date, env):
    """No broker authority. Wake the existing serial session child on a fresh quote."""
    from trader.us.market_calendar import now_ny,is_us_regular_market_open
    candidates = [w for w in watch if isinstance(w,dict) and holding_identity(w.get("identity"))
                  and isinstance(w.get("exchange"),str) and w["exchange"]]
    if env!="practice" or not candidates:
        return None
    cursor=0
    last_prices={}
    def poll():
        nonlocal cursor
        now=now_ny()
        if now.date().isoformat()!=str(trade_date) or not is_us_regular_market_open(now):
            return None
        batch,cursor=_bounded_quotes(candidates,cursor,env,
            pair=lambda item:(item["identity"]["symbol"],item["exchange"]))
        profit_wakeup=None
        for candidate,q in batch:
            try:
                entry,hard=float(candidate["entry"]),float(candidate["hard"])
                if not (math.isfinite(entry) and entry>0 and math.isfinite(hard) and 0<hard<1):
                    continue
                soft=candidate.get("soft")
                if (_breached_quote(q,entry,hard) is not None
                        or (isinstance(soft,(int,float)) and 0<soft<hard
                            and _breached_quote(q,entry,soft) is not None)):
                    return dict(candidate["identity"])
                # A fresh positive quote can only wake the complete SELL
                # lane; no shortcut around its TP/defense/stage arbitration.
                valid=_fresh_collector_quote(q)
                if valid is not None:
                    price,_=valid
                    symbol=candidate["identity"]["symbol"]
                    before=last_prices.get(symbol)
                    last_prices[symbol]=price
                    if profit_wakeup is None and profit_threshold_crossed(
                            candidate.get("profit"),price,previous=before):
                        profit_wakeup={"kind":"profit","identity":dict(candidate["identity"])}
            except (ValueError,TypeError,KeyError,OverflowError):
                continue
        return profit_wakeup
    return poll


def execute_hard_stop_interrupt(snapshot, *, trade_date, context, evaluate,
                                classify_family, suppress, route):
    """Main Tick thread only; all authority is delegated to existing gates.

    A read-only alert cannot itself be routed. A fresh canonical hard-stop
    decision for exactly this holding must survive the strict pending check.
    No fallback order or automatic retry is allowed, including on exceptions.
    """
    from trader.us.market_calendar import now_ny
    clock = now_ny()
    if not context.broker_submit_allowed() or not snapshot_still_fresh(snapshot,trade_date,clock):
        return [], None
    identity=holding_identity(snapshot)
    if identity is None:
        return [],None
    family=snapshot.get("_protective_trigger_family","HARD_STOP")
    def matches(intents):
        if (len(intents)!=1 or str(intents[0].get("side") or "").upper()!="SELL"
                or holding_identity(intents[0])!=identity):
            return False
        kind=str(intents[0].get("exit_type") or "").lower()
        selected=classify_family(intents[0])
        if family=="HARD_STOP":
            return kind in {"hard_stop","hard_stop_loss"} and selected=="HARD_STOP"
        if family=="CONFIRMED_SOFT_STOP":
            return ((kind=="soft_stop_loss" and selected=="SOFT_STOP")
                    or (kind=="persistent_soft_stop_full_exit" and selected=="HARD_STOP"))
        return False
    intents = evaluate(snapshot,clock)
    if not matches(intents):
        return [], None
    selected = suppress(intents)
    if (not matches(selected) or not context.broker_submit_allowed()
            or not snapshot_still_fresh(snapshot,trade_date,now_ny())):
        return [], None
    return selected, route(selected)


def route_idle_hard_stop_once(intents, *, route):
    """An exception after the canonical router starts is unresolved, not retryable."""
    import logging
    try:
        return route(intents)
    except Exception:
        logging.getLogger(__name__).exception("[US_EXIT][IDLE_WAKEUP][ROUTE_UNCERTAIN]")
        return {"orders":[{"status":"BROKER_SUBMIT_RESULT_UNKNOWN",
                           "requires_reconcile":True,"intent":intent}
                          for intent in intents],"decisions":[]}


def remaining_idle_watch(watch, attempted_identity, result):
    """Only advisory hints survive; each new child independently reconciles."""
    if (not isinstance(result,dict) or result.get("protective_wakeup") is not True
            or result.get("exit_route_reconcile_required") or result.get("manual_reconcile_required")
            or result.get("status") not in {"OK_NO_TRADE","OK_EXIT_ORDERS_SENT"}
            or result.get("entry_intents")!=0 or result.get("buy_orders")!=0
            or holding_identity(attempted_identity)!=attempted_identity):
        return []
    if result["status"]=="OK_EXIT_ORDERS_SENT":
        if not (result.get("orders_sent")==result.get("orders_ack")==1
                and result.get("exit_intents")==1 and result.get("reason")=="protective_wakeup_routed"):
            return []
    elif not (result.get("orders_sent")==result.get("orders_ack")==0
              and result.get("exit_intents")==0 and result.get("reason") in
              {"protective_wakeup_no_fresh_signal","protective_wakeup_holding_changed"}):
        return []
    return [row for row in watch if isinstance(row,dict)
            and holding_identity(row.get("identity")) is not None
            and row["identity"]["symbol"]!=attempted_identity["symbol"]]


def remaining_profit_idle_watch(watch, attempted_identity, result):
    """Retain unrelated read-only alerts after a verified SELL-only wakeup.

    Every later child still revalidates live positions, Stage4, and claims.
    Unknown/incomplete outcomes stop accelerated wakeups for this slot.
    """
    if (not isinstance(result,dict) or result.get("protective_wakeup") is not True
            or result.get("protective_wakeup_kind")!="profit"
            or result.get("exit_route_reconcile_required") is not False
            or result.get("manual_reconcile_required")!=0
            or result.get("entry_intents")!=0 or result.get("buy_orders")!=0
            or holding_identity(attempted_identity)!=attempted_identity
            or not isinstance(result.get("orders"),list)):
        return []
    orders=result["orders"]
    excluded={attempted_identity["symbol"]}
    if result.get("status")=="OK_NO_TRADE":
        if (orders or result.get("reason")!="protective_profit_wakeup_no_trade"
                or not result.get("orders_sent")==result.get("orders_ack")==result.get("exit_intents")==0):
            return []
    elif result.get("status")=="OK_EXIT_ORDERS_SENT":
        if (not orders or result.get("reason")!="protective_profit_wakeup_routed"
                or not len(orders)==result.get("orders_sent")==result.get("orders_ack")==result.get("exit_intents")):
            return []
        identities={holding_identity(row.get("identity"))["symbol"]:row["identity"]
                    for row in watch if isinstance(row,dict) and holding_identity(row.get("identity"))}
        routed=set()
        for order in orders:
            if not isinstance(order,dict) or order.get("status")!="ACK" or order.get("requires_reconcile"):
                return []
            intent=order.get("intent")
            identity=holding_identity(intent)
            if (not identity or intent.get("side")!="SELL" or identity["symbol"] in routed
                    or identities.get(identity["symbol"])!=identity):
                return []
            routed.add(identity["symbol"])
        excluded.update(routed)
    else:
        return []
    return [row for row in watch if isinstance(row,dict) and holding_identity(row.get("identity"))
            and row["identity"]["symbol"] not in excluded]


def preserve_confirmed_soft_state(position,trade_date):
    """Accelerated profit evaluation must never advance/reset confirmation."""
    proof=_confirmed_soft_snapshot(position,trade_date,fresh_resolver=True)
    keys=("risk_state","soft_stop_breach_count","soft_stop_triggered_today",
          "soft_stop_partial_done","first_soft_stop_at","first_soft_stop_price")
    position.pop("_legacy_prior_soft_stop_execution_candidate",None)
    if proof is not None:
        for key in keys:
            position[key]=proof[0].get(key)
    else:
        # No stale or inferred soft-stop proof. Other verified HWM/trend
        # fields retain their existing independent evidence.
        row=deepcopy(_verified_risk_row(position,trade_date,fresh_resolver=True) or {})
        if row:
            # Preserve independent, verified lifecycle, trend and forward V3
            # handoff evidence even when no soft stop has been confirmed.
            row["soft_stop_breach_count"]=0
            state=dict(row.get("state") or {})
            state.pop("soft_stop_execution",None)
            row["state"]=state
        position.update(risk_state=row,soft_stop_breach_count=0,
                        soft_stop_triggered_today=False,soft_stop_partial_done=False,
                        first_soft_stop_at=None,first_soft_stop_price=None)
    return position


def profit_wakeup_result(orders, intents, *, trade_date, session, position_count, decisions, incomplete=False):
    """Truthful terminal result for a multi-symbol SELL-only invocation."""
    from trader.us.runner.trade_tick_runner import _routed_order_truth_counts,_routed_sell_notional
    sent,ack,rejected,blocked=_routed_order_truth_counts(orders)
    count=len({str(i.get("symbol") or "").upper() for i in intents if str(i.get("side") or "").upper()=="SELL"})
    uncertain=(incomplete or any(str(o.get("status") or "").upper()!="ACK"
               or o.get("requires_reconcile") or str((o.get("intent") or {}).get("side") or "").upper()!="SELL"
               for o in orders) or count!=ack or sent!=ack)
    return dict(status="WARN_RECONCILE_PENDING" if uncertain else "OK_EXIT_ORDERS_SENT" if ack else "OK_NO_TRADE",
        reason="protective_profit_wakeup_unverified" if uncertain else "protective_profit_wakeup_routed" if ack else "protective_profit_wakeup_no_trade",
        protective_wakeup=True,protective_wakeup_kind="profit",orders=orders,orders_sent=sent,orders_ack=ack,
        orders_rejected=rejected,orders_blocked=blocked,entry_intents=0,buy_orders=0,exit_intents=count,
        entry_can_proceed=False,exit_can_proceed=True,session=session,trade_date=trade_date,positions=position_count,
        sell_decisions_detail=decisions,sell_notional_routed=_routed_sell_notional(orders),
        exit_route_reconcile_required=uncertain,manual_reconcile_required=int(uncertain),
        unresolved_ack_count=ack,reconcile_liveness_status="RECONCILE_REQUIRED" if uncertain else "PENDING_SETTLEMENT" if ack else "OK")
