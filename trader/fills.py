from datetime import datetime
import csv, os

def append_fill(side, code, name, qty, price, odno, note="", reason=""):
    """
    체결 기록을 CSV로 저장
    side: "BUY" or "SELL"
    code: 종목 코드
    name: 종목 이름
    qty: 체결 수량
    price: 체결 단가
    odno: 주문번호
    note: 추가 메모
    """
    os.makedirs("fills", exist_ok=True)
    path = f"fills/fills_{datetime.now().strftime('%Y%m%d')}.csv"
    header = ["ts", "side", "code", "name", "qty", "price", "ODNO", "note", "reason"]
    row = [
        datetime.now().isoformat(),
        side,
        code,
        name,
        int(qty),
        float(price),
        str(odno),
        note,
        reason or "",
    ]
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(header)
        w.writerow(row)
