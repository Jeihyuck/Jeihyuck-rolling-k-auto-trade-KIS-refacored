"""Bounded HOT price opportunity evaluation: no broker routing or authority."""
from __future__ import annotations
import hashlib
import json
import logging
import math
import os

LOG = logging.getLogger(__name__)


def poll_hot_events(db, *, env, scope, trade_date, hot_pairs, now_et,
                    quote_reader, seen, base="runtime/private", live_clock=None):
    from trader.us.entry_timing.opening_observer import priority_manifest_path, load_bars
    from trader.us.entry_timing.opportunity import evaluate_price_opportunity, effective_mode
    from trader.us.market_calendar import regular_close_time_for_date, is_us_trading_day
    from datetime import date
    from zoneinfo import ZoneInfo
    ny=ZoneInfo("America/New_York")
    if env != "practice" or not scope or len(hot_pairs)>4 or now_et.tzinfo is None:
        return []
    now_et=now_et.astimezone(ny)
    day=date.fromisoformat(trade_date)
    if (now_et.date()!=day or not is_us_trading_day(day)
            or (now_et.hour,now_et.minute)<(10,0)
            or now_et.time()>=regular_close_time_for_date(day)
            or effective_mode()!="ENFORCE"):
        return []
    path=priority_manifest_path(env=env,scope=scope,trade_date=trade_date,base=base)
    try:
        if path.stat().st_size>65536:
            return []
        payload=json.loads(path.read_text(encoding="utf-8"))
        created,expires=float(payload["created_epoch"]),float(payload["expires_epoch"])
        if (payload.get("schema")!="US_PREMINUTE_PRIORITY_V1"
                or payload.get("env")!=env or payload.get("owner")!="US_STANDARD"
                or payload.get("trade_date")!=trade_date
                or payload.get("is_order_authorization") is not False
                or payload.get("scope_sha256")!=hashlib.sha256((env+":"+scope).encode()).hexdigest()
                or not (math.isfinite(created) and math.isfinite(expires))
                or not created<=now_et.timestamp()<=expires<=created+180.00001):
            return []
        inputs=payload["hot_event_inputs"]
        listed={(x["symbol"],x["exchange"]) for lane in ("hot","warm","cold") for x in payload[lane]}
        if not isinstance(inputs,dict):
            return []
    except (OSError,ValueError,TypeError,KeyError,AttributeError):
        return []
    eligible={pair for pair in hot_pairs if pair in listed and pair[0]!="TQQQ"}
    for key in list(seen):
        if key not in eligible:
            seen.pop(key,None)
    results=[]
    for symbol,exchange in sorted(eligible):
        candidate=inputs.get(symbol)
        if not isinstance(candidate,dict):
            continue
        try:
            q=quote_reader(symbol,exchange) or {}
            at=live_clock().astimezone(ny) if live_clock else now_et
            if at.date()!=day or at.time()>=regular_close_time_for_date(day) or at.timestamp()>expires:
                break
            received=float(q["received_at"])
            age=at.timestamp()-received
            bid,ask,last=(float(q[k]) for k in ("bid","ask","last"))
            if (not q.get("collector_generation") or not all(map(math.isfinite,(received,bid,ask,last)))
                    or not 0<=age<=5 or not 0<bid<=ask or last<=0
                    or q.get("stale") or q.get("suspect")):
                continue
            bars=load_bars(db,trade_date=trade_date,symbol=symbol,owner="US_STANDARD")
            min_rr=float(os.getenv("US_OPPORTUNITY_MIN_REFERENCE_RR","1.5"))
            max_stop=float(os.getenv("US_OPPORTUNITY_MAX_STOP_DISTANCE","0.025"))
            signature=hashlib.sha256(json.dumps(
                [candidate,bars,bid,ask,last,q["collector_generation"],min_rr,max_stop],
                sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()
            key=(symbol,exchange)
            if seen.get(key)==signature:
                continue
            decision=evaluate_price_opportunity(
                style=candidate["family"],trade_date=trade_date,now_et=at,
                verified_bars=bars,quote={"source":"KIS_WEBSOCKET_COLLECTOR","last":last,
                                          "bid":bid,"ask":ask,"age_sec":age},
                prep=candidate["prep"],feature_scope=(scope,symbol,exchange,"US_STANDARD"),
                min_rr=min_rr,max_stop_distance=max_stop,
            )
            seen[key]=signature
            event={"symbol":symbol,"exchange":exchange,"observed_at_epoch":at.timestamp(),
                   "quote_received_epoch":received,"decision":decision,"is_order_authorization":False}
            results.append(event)
            LOG.info("[US_HOT_EVENT][EVALUATED] symbol=%s state=%s reason=%s quote_age_sec=%.3f latest_completed=%s action=NO_ORDER",
                     symbol,decision["state"],decision["reason"],age,
                     decision.get("facts",{}).get("last_completed_bar"))
        except (ValueError,TypeError,KeyError,AttributeError,OSError):
            continue
    return results
