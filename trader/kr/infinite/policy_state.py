from datetime import date
from uuid import uuid4
def cycle_id(trade_date: date) -> str:
    return f"KRINF-{trade_date:%Y%m%d}-{uuid4().hex[:8]}"
def idempotency_key(cycle: str, trade_date: date, action: str, sequence: int|None=None) -> str:
    suffix=f":{sequence}" if sequence is not None else ""
    return f"KR_INFINITE_V1:{cycle}:{trade_date.isoformat()}:{action}{suffix}"
