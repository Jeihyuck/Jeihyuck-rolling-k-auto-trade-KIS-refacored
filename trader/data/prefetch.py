#!/usr/bin/env python3
"""
OHLCV Prefetch Wrapper
주말 빌드에서 OHLCV 데이터를 미리 가져와 DB에 저장

Usage:
    python -m trader.data.prefetch --days 300
"""
import sys
from pathlib import Path

# 프로젝트 루트 확인
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 기존 ohlcv_prefetch 모듈 임포트
from trader.ohlcv_prefetch import main

if __name__ == "__main__":
    main()
