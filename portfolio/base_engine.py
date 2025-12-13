from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any


class BaseEngine(ABC):
    """Shared interface for independent trading engines."""

    def __init__(self, name: str, capital: float) -> None:
        self.name = name
        self.capital = float(capital)
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def tag(self) -> str:
        return f"[{self.name.upper()}]"

    @abstractmethod
    def rebalance_if_needed(self) -> Any:
        ...

    @abstractmethod
    def trade_loop(self) -> Any:
        ...

    def _log(self, message: str) -> None:
        self.logger.info(f"{self.tag} {message}")
