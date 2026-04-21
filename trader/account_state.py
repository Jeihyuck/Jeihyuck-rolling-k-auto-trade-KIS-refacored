from __future__ import annotations

import os
from typing import Any

from trader.config import PAPER_MAX_CAPITAL_KRW


def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def resolve_env_name(env: str | None = None) -> str:
    raw = str(env or os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice").strip().lower()
    return raw or "practice"


def get_account_key(
    *,
    env: str | None = None,
    kis: Any | None = None,
    cano: str | None = None,
    product_code: str | None = None,
) -> str:
    env_name = resolve_env_name(env)
    cano_value = _digits_only(cano or getattr(kis, "CANO", None) or os.getenv("CANO") or "") or "unknown"
    product_value = _digits_only(product_code or getattr(kis, "ACNT_PRDT_CD", None) or os.getenv("ACNT_PRDT_CD") or "") or "unknown"
    return f"{env_name}:{cano_value}:{product_value}"


def get_masked_account_key(
    *,
    env: str | None = None,
    kis: Any | None = None,
    cano: str | None = None,
    product_code: str | None = None,
) -> str:
    env_name = resolve_env_name(env)
    cano_value = _digits_only(cano or getattr(kis, "CANO", None) or os.getenv("CANO") or "")
    product_value = _digits_only(product_code or getattr(kis, "ACNT_PRDT_CD", None) or os.getenv("ACNT_PRDT_CD") or "")
    masked_cano = f"***{cano_value[-4:]}" if len(cano_value) >= 4 else ("***" if cano_value else "unknown")
    masked_product = product_value or "unknown"
    return f"{env_name}:{masked_cano}:{masked_product}"


def expected_practice_capital_krw() -> int:
    raw = str(os.getenv("EXPECTED_PRACTICE_CAPITAL_KRW") or os.getenv("PAPER_MAX_CAPITAL_KRW") or PAPER_MAX_CAPITAL_KRW)
    return int(raw.replace(",", "").strip() or PAPER_MAX_CAPITAL_KRW)


def expected_initial_holdings() -> int:
    raw = str(os.getenv("EXPECTED_INITIAL_HOLDINGS") or "0").strip()
    return int(raw or "0")


def account_reset_mode() -> bool:
    return env_flag("ACCOUNT_RESET_MODE") or env_flag("RESET_PRACTICE_ACCOUNT")