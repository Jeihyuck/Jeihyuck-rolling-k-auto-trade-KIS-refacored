"""Historical KR practice BUY diagnosis: KIS inquiry only, NEVER orders/DB writes."""
from __future__ import annotations
from datetime import date
import hashlib
import json
import os
import sys


def main() -> int:
    if os.getenv("KIS_ENV","").lower()!="practice" or os.getenv("STRATEGY_ENV","").lower()!="practice":
        raise SystemExit("PRACTICE_ONLY")
    if "openapivts.koreainvestment.com" not in str(os.getenv("KIS_REST_URL") or ""):
        raise SystemExit("PRACTICE_URL_ONLY")
    from trader.account_state import get_account_key
    digest=hashlib.sha256(get_account_key(env="practice").encode()).hexdigest()
    if not digest.startswith("fef5d08b17"):
        raise SystemExit("OCT20_ACCOUNT_SCOPE_MISMATCH")
    from trader.kis_wrapper import KisAPI
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
    r=_daily_ccld_all_pages_for_date(KisAPI(kis_env="practice"),date(2026,10,19))
    if not isinstance(r,dict) or str(r.get("rt_cd") or "")!="0" or not r.get("_sep29_complete"):
        raise SystemExit("BROKER_ORDER_HISTORY_NOT_AUTHORITATIVE")
    rows=r.get("output1") or []
    out={}
    for code in ("192820","251970"):
        matched=[]
        for row in rows:
            if not isinstance(row,dict): continue
            symbol=str(row.get("pdno") or "").strip().lstrip("A").zfill(6)
            side=str(row.get("sll_buy_dvsn_cd") or "").strip()
            day=str(row.get("ord_dt") or "").strip().replace("-","")
            if symbol==code and day=="20261019" and side in {"02","2","BUY"}:
                matched.append({
                    "broker_order_no_exists":bool(str(row.get("odno") or "").strip()),
                    "broker_executed_qty":int(float(row.get("tot_ccld_qty") or 0)),
                    "broker_executed_notional_positive":float(row.get("tot_ccld_amt") or 0)>0,
                })
        out[code]={"matching_broker_buy_orders":len(matched),"broker_rows":matched}
    print(json.dumps({"trade_date":"2026-10-19","complete":True,
        "pages":r.get("_sep29_pages"),"total_rows":len(rows),
        "target_orders":out},indent=2,ensure_ascii=False))
    # An empty match is OBSERVATION, not permission to silently unlock.
    return 0

if __name__=="__main__":
    sys.exit(main())
