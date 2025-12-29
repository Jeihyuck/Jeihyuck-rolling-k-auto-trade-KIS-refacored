from __future__ import annotations

import os
from dataclasses import dataclass

TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "f", "no", "n", "off"}


@dataclass
class EnvFlag:
    name: str
    value: bool
    raw: str | None
    normalized: str | None
    default: bool
    valid: bool


def env_str(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw)


def parse_env_flag(name: str, default: bool = False) -> EnvFlag:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return EnvFlag(name=name, value=default, raw=raw, normalized=None, default=default, valid=True)
    normalized = str(raw).strip().lower()
    if normalized in TRUE_VALUES:
        return EnvFlag(name=name, value=True, raw=raw, normalized=normalized, default=default, valid=True)
    if normalized in FALSE_VALUES:
        return EnvFlag(name=name, value=False, raw=raw, normalized=normalized, default=default, valid=True)
    return EnvFlag(name=name, value=default, raw=raw, normalized=normalized, default=default, valid=False)


def env_bool(name: str, default: bool = False) -> bool:
    return parse_env_flag(name, default=default).value


def resolve_mode(raw: str | None) -> str:
    normalized = (raw or "").strip().upper()
    if normalized in {"LIVE", "EXECUTE", "EXECUTION"}:
        return "LIVE"
    if normalized in {"INTENT_ONLY", "INTENT", "DIAG", "DIAGNOSTIC"}:
        return "INTENT_ONLY"
    return "INTENT_ONLY"
