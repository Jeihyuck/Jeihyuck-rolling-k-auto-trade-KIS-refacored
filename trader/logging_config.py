# FILE: trader/logging_config.py
import logging
from pathlib import Path

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

FORMAT = "[%(asctime)s][%(levelname)s][%(name)s] %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=logging.INFO,
    format=FORMAT,
    datefmt=DATEFMT,
    handlers=[
        logging.FileHandler(LOG_DIR / "trader.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
