#!/usr/bin/env python3
"""Read-only, sanitized US order-permission diagnosis.

No KIS/DB I/O. A matching .env assignment is only a *candidate* source:
the process could inherit the same value. Never print arbitrary env values.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from trader.us.execution.order_permissions import resolve_us_order_permissions

_FLAG_KEYS = (
    "DRY_RUN", "DISABLE_LIVE_TRADING", "DISABLE_REAL_TRADING",
    "LIVE_TRADING_ENABLED", "US_LIVE_TRADING_ENABLED", "US_ORDER_ARMED",
    "SIGNAL_ONLY", "US_AGENT_ENABLED", "US_PAPER_TRADING_ENABLED",
)
_SAFE_ENUMS = ("KIS_ENV", "STRATEGY_ENV", "RUN_MODE", "STRATEGY_MODE")


def diagnose(session: str, environ: dict[str, str], env_text: str = "") -> dict:
    strategy_env = str(environ.get("STRATEGY_ENV", "practice")).lower()
    mode = environ.get("RUN_MODE") or environ.get("STRATEGY_MODE") or "TRADE"
    p = resolve_us_order_permissions(session, strategy_env, mode, environ)
    declared = set(re.findall(
        r"(?m)^\\s*(?:export\\s+)?([A-Z][A-Z0-9_]*)\\s*=",
        env_text,
    ))
    # Restricted to safe enum and boolean keys; secrets/account/token impossible.
    flags = {key: ("UNSET" if key not in environ
                   else "1" if str(environ[key]).strip().lower() in {"1", "true", "yes", "on"}
                   else "0" if str(environ[key]).strip().lower() in {"0", "false", "no", "off", ""}
                   else "INVALID_BOOLEAN")
             for key in _FLAG_KEYS}
    return {
        "session": session,
        "permission_allowed": p.allowed,
        "permission_reasons": p.reasons,
        "effective_safe_flags": flags,
        "effective_safe_modes": {
            key: str(environ.get(key, "UNSET"))[:48]
            for key in _SAFE_ENUMS
        },
        "env_file_declares_flags": sorted(k for k in _FLAG_KEYS if k in declared),
        "origin_verified": False,
        "origin_caveat": "effective values may originate from parent, deployment preflight, .env, or wrapper defaults",
        "no_secrets_logged": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True, choices=("am", "afternoon"))
    parser.add_argument("--env-file", default=".env")
    args = parser.parse_args()
    path = Path(args.env_file)
    try:
        content = path.read_text(encoding="utf-8") if path.is_file() else ""
    except OSError:
        content = ""
    print("[US_ORDER_PERMISSION][STARTUP] " + json.dumps(
        diagnose(args.session, dict(os.environ), content), sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
