from __future__ import annotations


def _is_kr_stock_code(code: str | None) -> bool:
    """한국장 6자리 숫자 종목 코드 판별"""
    text = str(code or "").strip()
    return len(text) == 6 and text.isdigit()


def _is_kis_balance_authoritative_empty(balance_snapshot: dict | None) -> bool:
    """
    KIS balance가 정상 조회되었고 output1=[]이면 한국장 보유는 0개.
    이 경우 ledger_reconstruct 등 fallback을 금지한다.
    """
    if not isinstance(balance_snapshot, dict):
        return False

    rt_cd = str(balance_snapshot.get("rt_cd") or "0").strip()
    if rt_cd not in {"", "0"}:
        return False

    output1 = balance_snapshot.get("output1")
    output2 = balance_snapshot.get("output2")

    if isinstance(output1, dict):
        output1_rows = [output1] if output1 else []
    elif isinstance(output1, list):
        output1_rows = [row for row in output1 if isinstance(row, dict)]
    else:
        output1_rows = []

    if output1_rows:
        return False

    if isinstance(output2, dict) and output2:
        return True
    return True
