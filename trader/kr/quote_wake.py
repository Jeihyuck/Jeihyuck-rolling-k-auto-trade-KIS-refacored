"""Bounded, read-only wake hints for the existing serial KR PB1 loop.

No independent order writer, no KIS REST requests, no carried-forward BUY
authorization. The normal next tick recomputes all original gates.
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, time
from math import isfinite

from trader.kr.opening_bar_contract import KST, evaluate_proof_for_candidate
from trader.kr.entry_opportunity_v2 import validate_live_quote
from trader.kr.executable_quote import verified_price, complete_book

_REASONS = {
    "ADAPTIVE_V3_PRICE_OUTSIDE_INTERVAL", "ORDER_PRICE_ABOVE_CONFIRMED_CAP",
    "V2_ASK_ABOVE_PLANNED_LIMIT", "STRUCTURAL_STOP_DISTANCE_TOO_FAR",
    "PULLBACK_NO_ROOM_VS_STOP", "INSUFFICIENT_ROOM_VS_STOP",
}


def _positive(value):
    try:
        v = float(value)
        return v if isfinite(v) and v > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def make_wait_target(*, code, style, setup_ok, trigger_ok, pivot, timing, now, account_scope):
    if (not setup_ok or timing.get("entry_policy") != "US_ALIGNED_V2"
            or timing.get("state") != "WAIT" or timing.get("reason") not in _REASONS
            or len(code) != 6 or not code.isascii() or not code.isdigit()
            or code == "122630" or not str(account_scope).startswith("practice:")
            or now.tzinfo is None):
        return None
    cap = _positive(timing.get("diagnostic_entry_ceiling") or timing.get("max_entry_price"))
    stop = _positive(timing.get("structural_stop_reference"))
    floor = _positive(timing.get("diagnostic_entry_floor"))
    if not cap or not stop or not stop < cap:
        return None
    if floor and not stop < floor <= cap:
        return None
    try:
        stamp = datetime.fromisoformat(timing["latest_bar_ts"])
        if stamp.tzinfo is None or not 0 <= (now-stamp).total_seconds() <= 180:
            return None
    except (KeyError, TypeError, ValueError):
        return None
    return {"code": code, "style": style, "setup_ok": True, "trigger_ok": bool(trigger_ok),
            "pivot": _positive(pivot), "account_scope": account_scope,
            "cap": cap, "stop": stop, "floor": floor, "latest_bar_ts": stamp.isoformat(),
            "created_at": now.isoformat(), "is_order_authorization": False}


class QuoteWakeState:
    def __init__(self):
        self.seen = OrderedDict()

    def accept(self, target, bar):
        key = (target["account_scope"], target["code"], bar)
        if key in self.seen:
            return False
        self.seen[key] = None
        while len(self.seen) > 512:
            self.seen.popitem(last=False)
        return True


def _local_quote(code):
    from trader.marketdata.kis_ws_price import get_kis_ws_price_service
    from trader.marketdata.kr_ws_shared import read_owner_quote
    service = get_kis_ws_price_service()
    q = service.get_fresh_quote("KR", code, max_age_sec=5)
    # A fresh trade-only frame is not an executable book. Consult the
    # existing bounded shared WS owner rather than hiding its complete quote.
    if not verified_price(q, code, now=datetime.now(KST).timestamp()) or not complete_book(q):
        _, q = read_owner_quote(service, code, max_age_sec=5, timeout_sec=.10,
                                required_fields=("last", "bid", "ask"))
    return q


def probe_wait_target(target, now, *, read_quote=_local_quote):
    if (target.get("is_order_authorization") is not False
            or now.tzinfo is None or not time(9,30) <= now.astimezone(KST).time() <= time(15,5)):
        return None
    try:
        created = datetime.fromisoformat(target["created_at"])
        if created.date() != now.astimezone(KST).date() or not 0 <= (now-created).total_seconds() <= 180:
            return None
        q = read_quote(target["code"])
        if not verified_price(q, target["code"], now=now.timestamp()) or not complete_book(q):
            return None
        ask = _positive(q["ask"])
        if not ask or not target["stop"] < ask <= target["cap"] or (target["floor"] and ask < target["floor"]):
            return None
        # The previous diagnostic cap only filters work. Evaluate native
        # current bars and actual quote again, never turn a cap into approval.
        timing = evaluate_proof_for_candidate(
            env="practice", account_scope=target["account_scope"], code=target["code"],
            now=now, style=target["style"], setup_ok=target["setup_ok"],
            trigger_ok=target["trigger_ok"], pivot=target["pivot"], order_price=ask,
        )
        check = validate_live_quote(code=target["code"], timing=timing, quote=q,
                                    now=now, order_price=ask)
        if check["state"] != "READY":
            return None
        return {"code": target["code"], "latest_bar_ts": timing["latest_bar_ts"],
                "is_order_authorization": False}
    except (KeyError, ValueError, TypeError, OSError, OverflowError):
        return None


def make_protective_target(evaluation, now):
    """Existing routed PB1 SWING hard stop only; hint, never a SELL decision."""
    try:
        code = evaluation["code"]
        facts = evaluation["condition_details"]["paper_stop36"]
        stop = _positive(facts.get("effective_hard_stop"))
        mark = _positive(evaluation.get("current_price"))
        if (now.tzinfo is None or not isinstance(code, str)
                or len(code) != 6 or not code.isascii() or not code.isdigit()
                or code == "122630" or facts.get("eligible") is not True
                or evaluation.get("exit_policy_family") != "SWING_STAGED_EXIT"
                or evaluation.get("exit_ok") is not False
                or evaluation.get("signal_hit") or evaluation.get("submit_attempted")
                or evaluation.get("order_skip_reasons")
                or not _positive(evaluation.get("orderable_qty"))
                or not stop or not mark or mark <= stop):
            return None
        return {"code": code, "stop": stop, "created_at": now.isoformat(),
                "is_order_authorization": False}
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def _protective_quote(code):
    from trader.marketdata.kis_ws_price import get_kis_ws_price_service
    from trader.marketdata.kr_ws_shared import read_owner_quote
    service = get_kis_ws_price_service()
    q = service.get_fresh_quote("KR", code, max_age_sec=5)
    if not verified_price(q, code, now=datetime.now(KST).timestamp()):
        _, q = read_owner_quote(service, code, max_age_sec=5, timeout_sec=.10,
                                required_fields=("last",))
    return q


def probe_protective_targets(targets, now, *, read_quote=_protective_quote):
    """Fresh same-day WS marks only, bounded to ten existing holdings."""
    if now.tzinfo is None or not time(9, 0) <= now.astimezone(KST).time() <= time(15, 5):
        return None
    for target in targets[:10]:
        try:
            stamp = datetime.fromisoformat(target["created_at"])
            if (target.get("is_order_authorization") is not False
                    or stamp.tzinfo is None or stamp.astimezone(KST).date() != now.astimezone(KST).date()
                    or not 0 <= (now-stamp).total_seconds() <= 180
                    or target["code"] == "122630"):
                continue
            q = read_quote(target["code"])
            if (not verified_price(q, target["code"], now=now.timestamp())
                    or q.get("source") != "KIS_WEBSOCKET"):
                continue
            mark = _positive(q.get("last") or q.get("prpr") or q.get("stck_prpr"))
            stop = _positive(target["stop"])
            if mark and stop and mark <= stop:
                return {"code":target["code"], "kind":"PROTECTIVE_HARD_STOP_HINT",
                        "is_order_authorization":False}
        except (KeyError, ValueError, TypeError, OSError, AttributeError, OverflowError):
            continue
    return None


def wait_for_quote_wake(*, targets, state, seconds, session_end, now, monotonic, sleep,
                        probe=probe_wait_target, protective_targets=(),
                        protective_probe=probe_protective_targets):
    deadline = monotonic() + max(0.0, float(seconds))
    while True:
        remaining = min(deadline-monotonic(), (session_end-now()).total_seconds())
        if remaining <= 0:
            return None
        # Protection takes priority over entry hints. A hint wakes the same
        # serial owner for fresh holdings, broker and policy evaluation.
        if protective_targets:
            try:
                result = protective_probe(protective_targets, now())
                if monotonic() >= deadline or now() >= session_end:
                    return None
                if result:
                    return result
            except Exception:
                pass
        # At most three selected symbols, no new subscription or REST I/O.
        for target in targets[:3]:
            if monotonic() >= deadline or now() >= session_end:
                return None
            try:
                result = probe(target, now())
                # Native proof evaluation can consume the remainder of the idle
                # interval. Never accept or consume dedupe for a late wake.
                if monotonic() >= deadline or now() >= session_end:
                    return None
                if (result and result.get("code") == target["code"]
                        and state.accept(target, result["latest_bar_ts"])):
                    return result
            except Exception:
                # Optional wake must never interrupt normal safe schedule.
                continue
        remaining = min(deadline-monotonic(), (session_end-now()).total_seconds())
        if remaining > 0:
            sleep(min(1.0, remaining))


class ProtectiveYield:
    """Cooperative read-only checkpoint; never interrupt an in-flight order.

    The single writer abandons only the not-yet-submitted entry computation,
    settles the tick normally, then runs a fresh authoritative tick.
    """
    def __init__(self, targets, *, probe=probe_protective_targets, interval=1.0):
        self.targets = tuple(targets[:10])
        self.probe = probe
        self.interval = max(1.0, float(interval))
        self.next_poll = float("-inf")
        self.hit = None

    def check(self, *, now, monotonic):
        if self.hit is not None:
            return self.hit
        if not self.targets or monotonic < self.next_poll:
            return None
        self.next_poll = monotonic + self.interval
        try:
            hit = self.probe(self.targets, now)
            if (isinstance(hit, dict)
                    and hit.get("kind") == "PROTECTIVE_HARD_STOP_HINT"
                    and hit.get("is_order_authorization") is False
                    and hit.get("code") in {t["code"] for t in self.targets}):
                self.hit = hit
        except Exception:
            # Optional WS hints cannot break the normal broker/entry flow.
            pass
        return self.hit


# Only the optional daily-price bulk SELECT may use this lane. One outstanding
# detached read per process; it cannot submit orders or mutate engine state.
from threading import BoundedSemaphore
_PRICE_READ_SLOT = BoundedSemaphore(1)


def protective_daily_read(loader, *, checkpoint, poll_sec=.25):
    """Return (completed, result), polling protection while a pure SELECT waits.

    The caller must supply a private, disposable DB engine inside loader,
    not a shared account/order repository. If a stop hint arrives, discard
    the read result and let the sole writer finalize the zero-order tick.
    Without a hint the original read completes/errors normally. No second
    worker is launched while an abandoned read is still outstanding.
    """
    import queue
    import threading

    if checkpoint():
        return False, None
    if not _PRICE_READ_SLOT.acquire(blocking=False):
        # Optional preload may be missed; never launch overlapping workers.
        return False, None
    output = queue.Queue(maxsize=1)

    def read():
        try:
            output.put((True, loader()))
        except BaseException as exc:
            output.put((False, exc))
        finally:
            _PRICE_READ_SLOT.release()

    try:
        threading.Thread(target=read, name="kr-pb1-price-daily-read", daemon=True).start()
    except BaseException:
        _PRICE_READ_SLOT.release()
        raise
    interval = max(.01, min(.5, float(poll_sec)))
    while True:
        if checkpoint():
            return False, None
        try:
            ok, result = output.get(timeout=interval)
        except queue.Empty:
            continue
        if checkpoint():
            return False, None
        if not ok:
            raise result
        return True, result
