"""Read-only opening analysis shared by KR/US adapters. Never an order signal.

Callers supply verified native completed bars with canonical timezone-aware
time_et (US) or time_kst (KR), OHLC, volume and optional native turnover.
Strategy-owned target/support must be supplied explicitly; a missing value
cannot produce an executable price or authorize any trade.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from math import isfinite
from zoneinfo import ZoneInfo

def _num(value):
    try:
        v = float(value)
        return v if isfinite(v) else None
    except (TypeError, ValueError, OverflowError):
        return None


def analyze_opening_range(*, market, trade_date, now, bars, quote=None,
                          support=None, target=None, min_rr=1.5,
                          max_risk=.025, other_cap=None, tick_size=None):
    if market not in {"KR", "US"} or now.tzinfo is None:
        raise ValueError("OPENING_SCOPE_INVALID")
    zone = ZoneInfo("Asia/Seoul" if market == "KR" else "America/New_York")
    local = now.astimezone(zone)
    start = local.replace(hour=9, minute=0 if market=="KR" else 30, second=0, microsecond=0)
    end = start + timedelta(minutes=30)
    out = {"market": market, "trade_date": trade_date, "state": "INSUFFICIENT_DATA",
           "order_authorization": False, "opening_high": None, "opening_low": None,
           "opening_observed_count": 0, "opening_expected_count": 0,
           "opening_missing_count": 0, "opening_vwap": None,
           "provisional_entry_ceiling": None, "latest_completed_at": None}
    if local.date().isoformat() != trade_date:
        out["state"] = "INVALIDATED"
        return out
    by_minute, conflicts = {}, set()
    for bar in bars or []:
        if not isinstance(bar, dict):
            continue
        try:
            at = datetime.fromisoformat(str(bar["time_et" if market=="US" else "time_kst"]))
            if at.tzinfo is None:
                continue
            at = at.astimezone(zone)
        except (KeyError, ValueError, TypeError):
            continue
        if at.second or at.microsecond or at < start or at+timedelta(minutes=1)>local or at.date()!=local.date():
            continue
        values = tuple(_num(bar.get(k)) for k in ("open","high","low","close","volume","turnover"))
        o,h,l,c,v,t = values
        if any(x is None for x in (o,h,l,c,v)) or l<=0 or v<0 or not l<=min(o,c)<=max(o,c)<=h:
            continue
        if at in by_minute and by_minute[at]!=values:
            conflicts.add(at)
        by_minute[at]=values
    clean = sorted((at,val) for at,val in by_minute.items() if at not in conflicts)
    opening = [(at,v) for at,v in clean if at<end]
    expected = min(30,max(0,int((local-start).total_seconds()//60)))
    out.update(opening_expected_count=expected, opening_observed_count=len(opening),
               opening_missing_count=max(0,expected-len(opening)), conflicts=len(conflicts))
    if not opening:
        return out
    high_at,hv=max(opening,key=lambda item:item[1][1])
    low_at,lv=min(opening,key=lambda item:item[1][2])
    high,low=hv[1],lv[2]
    out.update(opening_high=high,opening_high_time=high_at.isoformat(),
               opening_low=low,opening_low_time=low_at.isoformat(),
               opening_range_pct=(high/low-1)*100,
               latest_completed_at=(clean[-1][0]+timedelta(minutes=1)).isoformat(),
               latest_completed_age_sec=(local-clean[-1][0]-timedelta(minutes=1)).total_seconds(),
               recent_local_high=max(v[1] for _,v in clean[-15:]),
               recent_local_low=min(v[2] for _,v in clean[-15:]),
               state="OPENING_RANGE_COMPLETE" if local>=end and len(opening)==30 else "OBSERVING")
    volume=sum(v[4] for _,v in opening)
    out["opening_volume"]=volume
    if volume>0 and all(v[5] is not None and v[5]>=0 for _,v in opening):
        vwap=sum(v[5] for _,v in opening)/volume
        if low<=vwap<=high:
            out["opening_vwap"]=vwap
    # No price-only samples become candles; do not derive an exit contract
    # from the opening low. Compute only an advisory strategy-supplied cap.
    support,target,min_rr,max_risk,other_cap,tick_size=map(_num,(support,target,min_rr,max_risk,other_cap,tick_size))
    if (support is not None and target is not None and min_rr is not None
            and max_risk is not None and tick_size is not None
            and 0<support<target and min_rr>=1 and 0<max_risk<1 and tick_size>0):
        from decimal import Decimal, ROUND_FLOOR
        cap=min((target+min_rr*support)/(1+min_rr),support/(1-max_risk))
        if other_cap is not None:
            cap=min(cap,other_cap)
        if cap>support:
            d=Decimal(str(tick_size))
            normalized=float((Decimal(str(cap))/d).to_integral_value(rounding=ROUND_FLOOR)*d)
            if normalized>support:
                out["provisional_entry_ceiling"]=normalized
    q=quote or {}
    out["quote_last_observed"]=_num(q.get("last"))
    out["quote_ask_observed"]=_num(q.get("ask"))
    return out
