"""Bounded HOT price opportunity evaluation: no broker routing or authority."""
from __future__ import annotations
from datetime import datetime, timedelta
import hashlib
import json
import logging
import math
import os

LOG = logging.getLogger(__name__)


def _execution_hot_pairs(payload):
    """Collection promotion alone cannot authorize an execution HOT hint."""
    inputs = payload["hot_event_inputs"]
    if not isinstance(inputs, dict):
        raise ValueError("invalid HOT inputs")
    symbols, hot = set(), set()
    for lane, cap in (("hot", 4), ("warm", 8), ("cold", 30)):
        entries = payload[lane]
        if not isinstance(entries, list) or len(entries) > cap:
            raise ValueError("invalid execution lane")
        for item in entries:
            symbol, exchange = item["symbol"], item["exchange"]
            if (not isinstance(symbol, str) or not symbol or symbol in symbols
                    or exchange not in {"NASDAQ", "NYSE", "AMEX"}):
                raise ValueError("invalid execution symbol")
            symbols.add(symbol)
            if lane == "hot" and symbol != "TQQQ" and isinstance(inputs.get(symbol), dict):
                hot.add((symbol, exchange))
    if len(symbols) > 30:
        raise ValueError("execution shortlist exceeds Final30")
    return hot


class CollectionRankState:
    """Thread-safe, advisory collection promotion; never edits execution manifests.

    Only a verified price-specific WAIT incumbent can yield one collection
    slot to a WARM challenger with two distinct fresh READY observations.
    Missing/stale data is never a reason to demote the incumbent.
    """
    def __init__(self):
        from threading import Lock
        self._lock=Lock()
        self._generation=None
        self._signature=None
        self._previous={}
        self._swap=None
        self._expires=0.0

    @staticmethod
    def _key(lanes):
        return tuple(tuple(lanes.get(tier,())) for tier in ("HOT","WARM","COLD"))

    def update(self, lanes, generation, observations, now_epoch):
        with self._lock:
            key=self._key(lanes)
            if self._generation!=generation or self._signature!=key:
                self._previous={}
                self._swap=None
            self._generation,self._signature=generation,key
            current={}
            for pair,obs in observations.items():
                if not isinstance(obs,dict):
                    continue
                try:
                    received=float(obs["received"])
                    if (not 0<=now_epoch-received<=5 or not obs.get("receipt_id")
                            or obs["state"] not in {"READY","PRICE_WAIT"}):
                        continue
                    current[pair]=obs
                except (ValueError,TypeError,KeyError):
                    continue
            warm=[p for p in lanes.get("WARM",()) if current.get(p,{}).get("state")=="READY"]
            weak=[p for p in reversed(lanes.get("HOT",())) if current.get(p,{}).get("state")=="PRICE_WAIT"]
            chosen=None
            if weak:
                for candidate in warm:
                    prior=self._previous.get(candidate,{})
                    obs=current[candidate]
                    if (prior.get("state")=="READY" and prior.get("receipt_id")!=obs["receipt_id"]
                            and 0 < obs["received"]-prior.get("received",float("inf")) <= 15):
                        chosen=(weak[0],candidate)
                        break
                    if (self._swap==(weak[0],candidate) and now_epoch<=self._expires):
                        chosen=self._swap
                        break
            self._previous=current
            self._swap=chosen
            self._expires=min((current[p]["received"]+5 for p in chosen),default=0.0) if chosen else 0.0
            return chosen

    def apply(self, lanes, generation, now_epoch):
        with self._lock:
            if (not self._swap or generation!=self._generation
                    or self._key(lanes)!=self._signature or now_epoch>self._expires):
                return lanes
            incumbent,challenger=self._swap
            result={tier:list(lanes[tier]) for tier in ("HOT","WARM","COLD")}
            i=result["HOT"].index(incumbent)
            j=result["WARM"].index(challenger)
            result["HOT"][i],result["WARM"][j]=challenger,incumbent
            return result


def collection_manifest(*,env,scope,trade_date,now_epoch,base="runtime/private"):
    """Read the existing canonical identity; do not renew or authorize it."""
    from trader.us.entry_timing.opening_observer import priority_manifest_path
    try:
        path=priority_manifest_path(env=env,scope=scope,trade_date=trade_date,base=base)
        if path.stat().st_size>65536:
            return None
        p=json.loads(path.read_text(encoding="utf-8"))
        created,expires=float(p["created_epoch"]),float(p["expires_epoch"])
        if (env!="practice" or not scope or p.get("schema")!="US_PREMINUTE_PRIORITY_V1"
                or p.get("env")!=env or p.get("owner")!="US_STANDARD" or p.get("trade_date")!=trade_date
                or p.get("is_order_authorization") is not False
                or p.get("scope_sha256")!=hashlib.sha256((env+":"+scope).encode()).hexdigest()
                or not isinstance(p.get("generation"),str) or not p["generation"]
                or not all(map(math.isfinite,(created,expires,now_epoch)))
                or not created<=now_epoch<=expires<=created+180.00001):
            return None
        _execution_hot_pairs(p)
        return p
    except (OSError,ValueError,TypeError,KeyError,AttributeError,OverflowError):
        return None


def refresh_collection_rank(db, *, rank, lanes, env, scope, trade_date, now_et,
                            quote_many, base="runtime/private", live_clock=None):
    """One bounded collector IPC batch, pure strategy evaluation, no order output."""
    from trader.us.entry_timing.opening_observer import load_bars
    from trader.us.entry_timing.opportunity import evaluate_price_opportunity,effective_mode
    import time
    from trader.us.market_calendar import is_us_trading_day, regular_close_time_for_date
    from zoneinfo import ZoneInfo
    now=now_et.astimezone(ZoneInfo("America/New_York")) if now_et.tzinfo else None
    if (now is None or now.date().isoformat()!=trade_date or not is_us_trading_day(now.date())
            or (now.hour,now.minute)<(10,0) or now.time()>=regular_close_time_for_date(now.date())
            or effective_mode()!="ENFORCE"):
        return None
    p=collection_manifest(env=env,scope=scope,trade_date=trade_date,now_epoch=now.timestamp(),base=base)
    if not p:
        return None
    allowed={(x["symbol"],x["exchange"]) for lane in ("hot","warm") for x in p[lane]}
    pairs=[pair for pair in lanes["HOT"]+lanes["WARM"]
           if pair in allowed and pair[0]!="TQQQ" and isinstance(p["hot_event_inputs"].get(pair[0]),dict)]
    if not pairs or len(pairs)>12:
        return None
    deadline=time.monotonic()+.8
    quotes=quote_many(pairs) or {}
    observations={}
    price_wait={"spread_exceeds_limit","live_price_far_from_completed_bar",
                "breakout_chase_too_large","reversal_not_confirmed","breakout_not_confirmed",
                "stop_geometry_invalid","stop_reference_too_far",
                "unfavorable_reference_reward_risk","entry_ceiling_below_ask"}
    for symbol,exchange in pairs:
        if time.monotonic()>=deadline:
            break
        at=live_clock() if live_clock else now
        try:
            q=quotes.get((symbol,exchange)) or {}
            received=float(q["received_at"])
            bid,ask,last=(float(q[k]) for k in ("bid","ask","last"))
            if (q.get("source")!="KIS_WEBSOCKET_COLLECTOR" or not q.get("collector_generation")
                    or q.get("stale") or q.get("suspect") or not all(map(math.isfinite,(received,bid,ask,last)))
                    or not 0<=at.timestamp()-received<=5 or not 0<bid<=ask or last<=0):
                continue
            c=p["hot_event_inputs"][symbol]
            decision=evaluate_price_opportunity(
                style=c["family"],trade_date=trade_date,now_et=at,
                verified_bars=load_bars(db,trade_date=trade_date,symbol=symbol,owner="US_STANDARD"),
                quote={"source":q["source"],"last":last,"bid":bid,"ask":ask,"age_sec":at.timestamp()-received},
                prep=c["prep"],feature_scope=(scope,symbol,exchange,"US_STANDARD"),
                min_rr=float(os.getenv("US_OPPORTUNITY_MIN_REFERENCE_RR","1.5")),
                max_stop_distance=float(os.getenv("US_OPPORTUNITY_MAX_STOP_DISTANCE","0.025")))
            state="READY" if decision["state"]=="READY" else "PRICE_WAIT" if decision.get("reason") in price_wait else None
            if state:
                observations[(symbol,exchange)]={"state":state,"received":received,
                        "receipt_id":(q["collector_generation"],received)}
        except (ValueError,TypeError,KeyError,AttributeError,OSError,OverflowError):
            continue
    at=live_clock() if live_clock else now
    if at.date()!=now.date() or at.time()>=regular_close_time_for_date(now.date()):
        return None
    current=collection_manifest(env=env,scope=scope,trade_date=trade_date,now_epoch=at.timestamp(),base=base)
    if not current or current["generation"]!=p["generation"]:
        return None
    return rank.update(lanes,p["generation"],observations,at.timestamp())


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
        generation=payload["generation"]
        if not isinstance(generation,str) or not generation:
            return []
        listed=_execution_hot_pairs(payload)
    except (OSError,ValueError,TypeError,KeyError,AttributeError):
        return []
    eligible={pair for pair in hot_pairs if pair in listed and pair[0]!="TQQQ"}
    for key in list(seen):
        if key not in eligible:
            seen.pop(key,None)
    results=[]
    def revoke_hint(symbol, exchange, at):
        # Invalid native evidence must not leave a previously READY hint live.
        # Never delete a newer observation or a different manifest generation.
        db.execute("""DELETE FROM hot_retry_events WHERE trade_date=? AND symbol=?
            AND exchange=? AND manifest_generation=? AND observed_epoch<=?""",
            (trade_date,symbol,exchange,generation,at.timestamp()))
        db.commit()
        seen.pop((symbol,exchange),None)
    for symbol,exchange in sorted(eligible):
        candidate=inputs.get(symbol)
        if not isinstance(candidate,dict):
            continue
        at=now_et
        try:
            q=quote_reader(symbol,exchange) or {}
            at=live_clock().astimezone(ny) if live_clock else now_et
            if at.date()!=day or at.time()>=regular_close_time_for_date(day) or at.timestamp()>expires:
                break
            received=float(q["received_at"])
            age=at.timestamp()-received
            bid,ask,last=(float(q[k]) for k in ("bid","ask","last"))
            if (q.get("source")!="KIS_WEBSOCKET_COLLECTOR"
                    or not q.get("collector_generation") or not all(map(math.isfinite,(received,bid,ask,last)))
                    or not 0<=age<=5 or not 0<bid<=ask or last<=0
                    or q.get("stale") or q.get("suspect")):
                revoke_hint(symbol,exchange,at)
                continue
            bars=load_bars(db,trade_date=trade_date,symbol=symbol,owner="US_STANDARD")
            min_rr=float(os.getenv("US_OPPORTUNITY_MIN_REFERENCE_RR","1.5"))
            max_stop=float(os.getenv("US_OPPORTUNITY_MAX_STOP_DISTANCE","0.025"))
            signature=hashlib.sha256(json.dumps(
                [generation,candidate,bars,bid,ask,last,received,q["collector_generation"],min_rr,max_stop],
                sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()
            key=(symbol,exchange)
            if seen.get(key)==signature:
                continue
            decision=evaluate_price_opportunity(
                style=candidate["family"],trade_date=trade_date,now_et=at,
                verified_bars=bars,quote={"source":q["source"],"last":last,
                                          "bid":bid,"ask":ask,"age_sec":age},
                prep=candidate["prep"],feature_scope=(scope,symbol,exchange,"US_STANDARD"),
                min_rr=min_rr,max_stop_distance=max_stop,
            )
            finished = live_clock().astimezone(ny) if live_clock else at
            quote_latency = finished.timestamp() - received
            latest_bar = decision.get("facts", {}).get("last_completed_bar")
            close_latency = None
            if latest_bar:
                bar_open = datetime.fromisoformat(latest_bar)
                if bar_open.tzinfo is not None:
                    close_latency = (finished - (bar_open + timedelta(minutes=1))).total_seconds()
            LOG.info(
                "[US_HOT_EVENT][LATENCY] symbol=%s generation=%s state=%s "
                "received_to_evaluated_sec=%.3f bar_close_to_evaluated_sec=%s",
                symbol, generation, decision["state"], quote_latency, close_latency,
            )
            if (finished < at or finished.date() != day or finished.time() >= regular_close_time_for_date(day)
                    or finished.timestamp() > expires or not 0 <= quote_latency <= 5):
                revoke_hint(symbol, exchange, finished)
                continue
            at = finished
            # A local latest-state hint, not an order, does not carry a price
            # or executable decision. The canonical engine must independently
            # recalculate the strategy, size, risk and router permissions.
            db.execute("""INSERT INTO hot_retry_events
                (trade_date,symbol,exchange,manifest_generation,event_id,observed_epoch,quote_received_epoch,state)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(trade_date,symbol,exchange) DO UPDATE SET
                    manifest_generation=excluded.manifest_generation,event_id=excluded.event_id,
                    observed_epoch=excluded.observed_epoch,quote_received_epoch=excluded.quote_received_epoch,
                    state=excluded.state
                WHERE excluded.observed_epoch>=hot_retry_events.observed_epoch
                """,(trade_date,symbol,exchange,generation,signature,at.timestamp(),received,decision["state"]))
            db.commit()
            seen[key]=signature
            event={"symbol":symbol,"exchange":exchange,"observed_at_epoch":at.timestamp(),
                   "quote_received_epoch":received,"decision":decision,"is_order_authorization":False}
            results.append(event)
            LOG.info("[US_HOT_EVENT][EVALUATED] symbol=%s state=%s reason=%s quote_age_sec=%.3f latest_completed=%s action=NO_ORDER",
                     symbol,decision["state"],decision["reason"],age,
                     decision.get("facts",{}).get("last_completed_bar"))
        except (ValueError,TypeError,KeyError,AttributeError,OSError):
            revoke_hint(symbol,exchange,at)
            continue
    return results


def read_hot_retry_hints(*, env, scope, trade_date, generation, waiting,
                         now_epoch, base="runtime/private"):
    """Bounded read-only hints for a currently eligible, previously WAIT symbol."""
    import sqlite3
    from contextlib import closing
    from trader.us.entry_timing.opening_observer import store_path, priority_manifest_path
    if env!="practice" or not scope or not generation or not waiting or len(waiting)>30:
        return set()

    def eligible_pairs():
        # The canonical publisher may replace the shortlist while a Tick is
        # running. Do not consume orphaned hints from an old generation even
        # if their individual quote timestamp is still within five seconds.
        manifest=priority_manifest_path(env=env,scope=scope,trade_date=trade_date,base=base)
        if manifest.stat().st_size>65536:
            return set()
        payload=json.loads(manifest.read_text(encoding="utf-8"))
        created,expires=float(payload["created_epoch"]),float(payload["expires_epoch"])
        if (payload.get("schema")!="US_PREMINUTE_PRIORITY_V1"
                or payload.get("env")!=env or payload.get("owner")!="US_STANDARD"
                or payload.get("trade_date")!=trade_date
                or payload.get("is_order_authorization") is not False
                or payload.get("scope_sha256")!=hashlib.sha256((env+":"+scope).encode()).hexdigest()
                or payload.get("generation")!=generation
                or not all(map(math.isfinite,(created,expires,now_epoch)))
                or not created<=now_epoch<=expires<=created+180.00001):
            return set()
        inputs=payload["hot_event_inputs"]
        if not isinstance(inputs,dict):
            return set()
        return _execution_hot_pairs(payload) & set(waiting)

    try:
        path=store_path(env=env,account_scope=scope,trade_date=trade_date,base=base)
        if not path.is_file():
            return set()
        pairs=sorted(eligible_pairs())
        if not pairs:
            return set()
        # Apply eligibility before LIMIT, otherwise newer unrelated hints can
        # starve a canonical WAIT candidate. The input is bounded to Final30.
        pair_filter=" OR ".join("(symbol=? AND exchange=?)" for _ in pairs)
        params=[part for pair in pairs for part in pair]
        with closing(sqlite3.connect("file:"+str(path.resolve())+"?mode=ro",uri=True,timeout=.025)) as db:
            rows=db.execute("""SELECT symbol,exchange,observed_epoch,quote_received_epoch,event_id
                FROM hot_retry_events WHERE trade_date=? AND manifest_generation=? AND state='READY'
                AND observed_epoch BETWEEN ? AND ? AND quote_received_epoch BETWEEN ? AND ?
                AND observed_epoch>=quote_received_epoch AND observed_epoch-quote_received_epoch<=5
                AND length(event_id)>0 AND ("""+pair_filter+""")
                ORDER BY observed_epoch DESC,symbol,exchange LIMIT 4""",
                (trade_date,generation,now_epoch-5,now_epoch,now_epoch-5,now_epoch,*params)).fetchall()
        still_eligible=eligible_pairs()
        return {(symbol,exchange) for symbol,exchange,at,received,event_id in rows
                if (symbol,exchange) in still_eligible
                and event_id and 0<=now_epoch-at<=5 and 0<=at-received<=5
                and 0<=now_epoch-received<=5}
    except (OSError,ValueError,TypeError,KeyError,AttributeError,OverflowError,sqlite3.Error):
        return set()


def candidate_pair(candidate):
    from trader.us.symbols import normalize_us_exchange
    try:
        return (str(candidate[1]).strip().upper(),normalize_us_exchange(candidate[2]))
    except (ValueError,TypeError,IndexError):
        return None


def iter_candidates_with_hot_retries(candidates, *, waiting, ready_reader):
    """Re-evaluate only a previously WAIT eligible candidate, once per tick.

    Normal ranking and rank_final30 never change; no new symbols are injected.
    No sleeps and no second engine or submitter. Existing per-tick cash,
    candidate caps, cancellation and canonical order gates still apply.
    """
    ranked=list(enumerate(candidates))
    retried=set()
    def ready_retries():
        if not waiting or len(retried)>=4:
            return []
        hints=ready_reader(set(waiting)-retried)
        selected=[]
        for rank,candidate in ranked:
            key=candidate_pair(candidate)
            if key in hints and key in waiting and key not in retried:
                retried.add(key)
                waiting.discard(key)
                selected.append((rank,candidate))
                if len(retried)>=4:
                    break
        return selected
    for rank,candidate in ranked:
        yield from ready_retries()
        yield rank,candidate
    yield from ready_retries()


def make_hot_slot_poll(waiting, *, env, scope, trade_date, not_before_monotonic=0,
                       base="runtime/private", epoch_clock=None, monotonic=None):
    """A fresh prior-WAIT hint may advance ONE existing normal session slot.

    No order or engine runs here. The caller owns the slot and protective
    priority; a subsequent normal child must re-evaluate all canonical gates.
    """
    import time
    from trader.us.entry_timing.opportunity import effective_mode
    from trader.us.market_calendar import now_ny,is_us_regular_market_open
    if (env!="practice" or not scope or not isinstance(waiting,dict)
            or set(waiting)!={"generation","pairs"} or not isinstance(waiting["generation"],str)
            or not waiting["generation"] or not isinstance(waiting["pairs"],list)
            or not 1<=len(waiting["pairs"])<=4):
        return None
    pairs=set()
    for item in waiting["pairs"]:
        if (not isinstance(item,(tuple,list)) or len(item)!=2
                or not isinstance(item[0],str) or not item[0] or item[0]=="TQQQ"
                or item[1] not in ("NASDAQ","NYSE","AMEX")):
            return None
        pairs.add(tuple(item))
    if len(pairs)!=len(waiting["pairs"]):
        return None
    generation=waiting["generation"]
    clock=epoch_clock or time.time
    mono=monotonic or time.monotonic
    consumed=False
    def poll():
        nonlocal consumed
        if consumed or mono()<not_before_monotonic or effective_mode()!="ENFORCE":
            return None
        now=now_ny()
        if (now.date().isoformat()!=trade_date or (now.hour,now.minute)<(10,0)
                or not is_us_regular_market_open(now)):
            return None
        hints=read_hot_retry_hints(env=env,scope=scope,trade_date=trade_date,
            generation=generation,waiting=pairs,now_epoch=clock(),base=base)
        if not hints:
            return None
        consumed=True
        return {"kind":"hot_slot","generation":generation,"pairs":[list(p) for p in sorted(hints)]}
    return poll


def combine_idle_polls(protective,hot):
    """Always service protective SELL alerts first; no parallel writer."""
    if protective is None:
        return hot
    if hot is None:
        return protective
    def poll():
        result=protective()
        return result if result is not None else hot()
    return poll


def select_focused_hot_rows(rows, focus, *, env, scope, trade_date, now_epoch,
                            base="runtime/private"):
    """Revalidate idle advice after canonical account and portfolio filters.

    Missing/expired/replaced advice means no focused BUY evaluation. Preserve
    the original locked row and rank, never synthesize a new candidate.
    """
    from trader.us.entry_timing.opportunity import effective_mode
    from trader.us.symbols import normalize_us_exchange
    if (env!="practice" or not scope or effective_mode()!="ENFORCE"
            or not isinstance(focus,dict) or set(focus)!={"kind","generation","pairs"}
            or focus["kind"]!="hot_slot" or not isinstance(focus["generation"],str)
            or not focus["generation"] or not isinstance(focus["pairs"],list)
            or not 1<=len(focus["pairs"])<=4):
        return []
    pairs=set()
    for item in focus["pairs"]:
        if (not isinstance(item,(list,tuple)) or len(item)!=2
                or not isinstance(item[0],str) or not item[0] or item[0]=="TQQQ"
                or item[1] not in ("NYSE","NASDAQ","AMEX")):
            return []
        pairs.add(tuple(item))
    if len(pairs)!=len(focus["pairs"]):
        return []
    hints=read_hot_retry_hints(env=env,scope=scope,trade_date=trade_date,
        generation=focus["generation"],waiting=pairs,now_epoch=now_epoch,base=base)
    selected=[]
    for row in rows or []:
        try:
            symbol=str(row["symbol"]).upper().strip()
            exchange=normalize_us_exchange(row["exchange"])
            rank=row.get("rank_final30") or row.get("rank")
            if (symbol,exchange) in hints and type(rank) is int and 1<=rank<=30:
                selected.append(row)
        except (KeyError,ValueError,TypeError,AttributeError):
            continue
    return selected if len(selected)==len({str(row["symbol"]).upper().strip() for row in selected}) else []
