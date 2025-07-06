import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(app_name: str = "rolling_k") -> None:
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(exist_ok=True)

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers = [
        logging.StreamHandler(),
        RotatingFileHandler(
            log_dir / f"{app_name}.log",
            maxBytes=10_000_000,
            backupCount=5,
            encoding="utf-8",
        ),
    ]
    for h in handlers:
        h.setFormatter(logging.Formatter(fmt, datefmt=datefmt))

    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)

    # uvicorn 계열도 같은 핸들러
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers = handlers
        lg.setLevel(logging.INFO)
