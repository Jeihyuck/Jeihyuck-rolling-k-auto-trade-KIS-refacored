import logging
import requests

try:
    from settings import SLACK_WEBHOOK
except Exception:
    SLACK_WEBHOOK = ""
from .env import (
    EnvFlag,
    FALSE_VALUES,
    TRUE_VALUES,
    env_bool,
    env_str,
    parse_env_flag,
    resolve_mode,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def log(msg):
    logging.info(msg)


def send_slack(text):
    if not SLACK_WEBHOOK:
        return
    requests.post(SLACK_WEBHOOK, json={"text": text})


__all__ = [
    "EnvFlag",
    "FALSE_VALUES",
    "TRUE_VALUES",
    "env_bool",
    "env_str",
    "log",
    "parse_env_flag",
    "resolve_mode",
    "send_slack",
]
