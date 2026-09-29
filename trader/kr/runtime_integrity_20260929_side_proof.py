"""Fail-closed KIS side proof for Sep-29 cross-day BUY contract recovery.

Cross-day policy recovery is allowed only when the broker execution row explicitly
proves BUY. Missing or unknown side fields are not execution-side evidence.
"""
from __future__ import annotations

from typing import Any


def _explicit_buy_side(row: dict[str, Any]) -> bool:
    raw = str(
        row.get("sll_buy_dvsn_cd")
        or row.get("side")
        or row.get("sll_buy_dvsn_name")
        or ""
    ).strip().upper()
    if not raw:
        return False
    return raw in {"02", "BUY", "매수"} or "BUY" in raw or "매수" in raw


def install_kr_20260929_side_proof_guard() -> None:
    """Make cross-day execution proof fail closed when side is absent/unknown."""
    import trader.kr.runtime_integrity_20260929 as base

    if getattr(base, "_kr_p0_20260929_strict_side_proof_installed", False):
        return
    base._row_side_is_buy = _explicit_buy_side
    base._kr_p0_20260929_strict_side_proof_installed = True
