#!/usr/bin/env python3
"""DIAG 모드 KIS 호출 0회 검증 스크립트."""
import sys
import re
from pathlib import Path

FORBIDDEN_PATTERNS = [
    r"inquire-daily-itemchartprice",
    r"DAILY_FAIL",
    r"\[KIS\]\[HTTP_DISABLED\]",
    r"\[OHLCV\]\[KIS\]\[FALLBACK\]",
    r"kis\.get_daily_candles",
]

def check_kis_calls(content: str) -> tuple[bool, list[str]]:
    matches = []
    for pattern in FORBIDDEN_PATTERNS:
        if re.search(pattern, content, re.IGNORECASE):
            matches.append(pattern)
    return bool(matches), matches

def main():
    if len(sys.argv) > 1:
        log_path = Path(sys.argv[1])
        if not log_path.exists():
            print(f"❌ Error: Log file not found: {log_path}", file=sys.stderr)
            sys.exit(2)
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        source = str(log_path)
    else:
        content = sys.stdin.read()
        source = "stdin"
    
    has_calls, patterns = check_kis_calls(content)
    
    if has_calls:
        print("❌ DIAG 검증 실패: KIS HTTP 호출 흔적이 발견되었습니다!", file=sys.stderr)
        print(f"   소스: {source}", file=sys.stderr)
        print(f"   발견된 패턴: {', '.join(patterns)}", file=sys.stderr)
        print("", file=sys.stderr)
        print("DIAG 모드에서는 KIS API를 절대 호출하면 안 됩니다.", file=sys.stderr)
        print("DB → KRX 순서로만 데이터를 가져와야 합니다.", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"✅ DIAG 검증 통과: KIS HTTP 호출 없음 (소스: {source})")
        sys.exit(0)

if __name__ == "__main__":
    main()
