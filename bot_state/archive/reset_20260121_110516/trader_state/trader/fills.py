from datetime import datetime
from typing import Any

from .fill_store import append_fill as append_fill_jsonl
from .strategy_registry import normalize_sid


def append_fill(side, code, name, qty, price, odno, note: str = "", reason: str = "", sid: Any | None = None):
    """
    체결 기록을 JSONL로 저장 (backward compatibility wrapper).
    """
    append_fill_jsonl(
        ts=datetime.now().isoformat(),
        order_id=odno,
        pdno=str(code),
        sid=normalize_sid(sid),
        side=str(side).upper(),
        qty=int(qty),
        price=float(price),
        source=reason or "append_fill",
        note=note,
    )
