import logging
from logging.handlers import RotatingFileHandler
import os

LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # 콘솔 핸들러 (INFO 이상)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
        "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(ch)

    # 파일 핸들러 (DEBUG 이상, 회전)
    fh = RotatingFileHandler(
        os.path.join(LOG_DIR, "app.log"),
        maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(name)s:%(lineno)d] %(message)s",
        "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)
