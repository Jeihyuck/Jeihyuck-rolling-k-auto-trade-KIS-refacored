from __future__ import annotations

from typing import Any, Dict


class BaseStrategy:
    """Lightweight base strategy interface for intent generation."""

    name: str = ""
    sid: int = 0

    def update_state(self, market_data: Dict[str, Any]) -> None:  # pragma: no cover - stub
        return

    def should_enter(self, market_data: Dict[str, Any], portfolio_state: Dict[str, Any]) -> bool:
        return False

    def compute_entry(
        self, market_data: Dict[str, Any], portfolio_state: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        return None

    def should_exit(
        self, position: Dict[str, Any], market_data: Dict[str, Any], portfolio_state: Dict[str, Any]
    ) -> bool:
        return False

    def compute_exit(
        self, position: Dict[str, Any], market_data: Dict[str, Any], portfolio_state: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        return None
