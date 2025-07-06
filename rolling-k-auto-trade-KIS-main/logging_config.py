# logging_config.py

import logging
import logging.handlers as lh
from datetime import datetime
import pathlib

def setup_logging(app_name: str = "rolling_k") -> None:
    log_dir = pathlib.Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"{app_name}_{datetime.now().strftime('%Y-%m-%d')}.log"

    fmt = "[%(asctime)s] %(levelname)-8s | %(name)s: %(message)s"
    formatter = logging.Formatter(fmt, "%Y-%m-%d %H:%M:%S")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    # 콘솔
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # 파일 핸들러 (회전)
    fh = lh.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

setup_logging()
