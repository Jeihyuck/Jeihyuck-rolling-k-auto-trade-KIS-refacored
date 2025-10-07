import logging
import sys
import os
import json as _json
from datetime import datetime
from typing import Optional, Union

# --------------------------------------------------------------------------------------
# Logging with KST timestamps, optional JSON output, and env-driven defaults
# - LOG_LEVEL: DEBUG/INFO/WARN/ERROR (default: INFO)
# - LOG_JSON:  "1/true/yes/on" to enable JSON logs (default: false)
# - LOG_FILE:  path to also write logs to a file (optional)
# --------------------------------------------------------------------------------------

_FMT = "[%(asctime)s][%(levelname)s][%(name)s] %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _bool_env(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "y", "yes", "on"}


class _KstTZ(logging.Formatter):
    """Text formatter that renders %(asctime)s in KST."""

    def converter(self, timestamp):
        import pytz
        kst = pytz.timezone("Asia/Seoul")
        return datetime.fromtimestamp(timestamp, tz=kst)

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime(_DATEFMT)


class _JsonFormatter(logging.Formatter):
    """Lightweight JSON log formatter with KST ISO8601 timestamps."""

    def __init__(self, datefmt: Optional[str] = None):
        super().__init__()
        self.datefmt = datefmt

    def format(self, record: logging.LogRecord) -> str:
        # Base fields
        payload = {
            "ts": self._format_ts(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Useful context
        payload.update({
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "pid": record.process,
            "thread": record.threadName,
        })
        # Attach extra dict if present (fields added via logger.info(..., extra={...}))
        # Avoid clobbering base fields
        for k, v in getattr(record, "__dict__", {}).items():
            if k.startswith("_"):
                continue
            if k in payload:
                continue
            # skip standard attributes of LogRecord
            if k in (
                "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
                "exc_info", "exc_text", "stack_info", "lineno", "funcName", "created", "msecs",
                "relativeCreated", "thread", "threadName", "processName", "process",
            ):
                continue
            payload[k] = v

        # Exception/stack if any
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        return _json.dumps(payload, ensure_ascii=False)

    def _format_ts(self, record: logging.LogRecord) -> str:
        import pytz
        kst = pytz.timezone("Asia/Seoul")
        dt = datetime.fromtimestamp(record.created, tz=kst)
        if self.datefmt:
            return dt.strftime(self.datefmt)
        # ISO8601 without microseconds
        return dt.replace(microsecond=0).isoformat()


def _coerce_level(level: Union[str, int, None]) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        # Accept numeric strings as well
        s = level.strip().upper()
        if s.isdigit():
            try:
                return int(s)
            except Exception:
                pass
        return getattr(logging, s, logging.INFO)
    return logging.INFO


def setup_logging(
    level: Union[str, int, None] = None,
    *,
    json: Optional[bool] = None,
    log_file: Optional[str] = None,
    quiet_libs: bool = True,
) -> None:
    """
    Configure root logger.

    Args:
        level: explicit level; if None, read from env LOG_LEVEL (default INFO)
        json:  enable JSON logs; if None, read from env LOG_JSON
        log_file: optional path to also write logs to a file
        quiet_libs: down-level noisy 3rd-party loggers
    """

    # Defaults from env if not provided
    env_level = os.getenv("LOG_LEVEL", "INFO") if level is None else level
    env_json = _bool_env(os.getenv("LOG_JSON"), False) if json is None else json
    log_path = os.getenv("LOG_FILE") if log_file is None else log_file

    lvl = _coerce_level(env_level)

    root = logging.getLogger()

    # Reset handlers to make this idempotent
    if root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)

    # Build handlers
    stream_h = logging.StreamHandler(sys.stdout)
    formatter = _JsonFormatter() if env_json else _KstTZ(_FMT)
    stream_h.setFormatter(formatter)
    root.addHandler(stream_h)

    if log_path:
        try:
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setFormatter(formatter)
            root.addHandler(fh)
        except Exception as e:
            # Fallback: log the failure to initialize file handler
            tmp = logging.StreamHandler(sys.stderr)
            tmp.setFormatter(_KstTZ(_FMT))
            root.addHandler(tmp)
            logging.getLogger(__name__).warning(f"[logging] file handler init failed: {e}")

    root.setLevel(lvl)

    if quiet_libs:
        # Common noisy libraries
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("uvicorn.access").setLevel(logging.INFO)
        logging.getLogger("uvicorn.error").setLevel(logging.INFO)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)
        logging.getLogger("websockets").setLevel(logging.WARNING)


# Convenience: auto-configure if directly imported/run and no handlers exist
if not logging.getLogger().handlers:
    setup_logging()
