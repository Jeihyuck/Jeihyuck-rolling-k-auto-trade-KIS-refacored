"""
로깅 유틸리티: JSONL 파일 작성, 카드 기록 등
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def append_jsonl(path: str, payload: dict) -> None:
    """
    JSONL 파일에 한 줄씩 JSON 객체 추가 (no crash mode)
    
    Args:
        path: 저장할 JSONL 파일 경로
        payload: 저장할 딕셔너리 (JSON serializable)
    
    동작:
        - 디렉토리 없으면 생성
        - 예외 발생 시 WARN만 로깅하고 메인 로직은 계속
    """
    try:
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # JSONL 포맷: newline delimited JSON
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(
            "[LOGGING_UTILS][JSONL_APPEND_FAIL] path=%s error=%s",
            path,
            type(e).__name__,
            exc_info=False
        )
        # 메인 로직은 계속하도록 함 (여기서는 return)
