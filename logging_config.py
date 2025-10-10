# logging_config.py
# 루트(상위 탑 디렉토리)에 위치. 모든 모듈에서 import 하도록 경로 주의.

import logging
import os
from logging.handlers import RotatingFileHandler

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_FILE = os.path.join(LOG_DIR, "app.log")
LOG_FMT = os.getenv("LOG_FMT", "[%(asctime)s] %(levelname)s %(name)s: %(message)s")
LOG_MAX_MB = int(os.getenv("LOG_MAX_MB", "10"))  # 파일 당 최대 크기(MB)
LOG_BACKUP = int(os.getenv("LOG_BACKUP", "5"))  # 보관 파일 개수

os.makedirs(LOG_DIR, exist_ok=True)

root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# 기존 핸들러 제거(중복 방지)
while root_logger.handlers:
    root_logger.handlers.pop()

# 콘솔 핸들러
ch = logging.StreamHandler()
ch.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
ch.setFormatter(logging.Formatter(LOG_FMT))
root_logger.addHandler(ch)

# 파일 핸들러(순환)
fh = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_MB*1024*1024, backupCount=LOG_BACKUP, encoding="utf-8")
fh.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
fh.setFormatter(logging.Formatter(LOG_FMT))
root_logger.addHandler(fh)

# 필요시 모듈별 수준 조정 예시
# logging.getLogger("uvicorn").setLevel(logging.WARNING)
# logging.getLogger("fastapi").setLevel(logging.INFO)

logging.info(f"[logging_config] LOG_FILE={LOG_FILE} LEVEL={LOG_LEVEL}")
