from __future__ import annotations

import logging
from typing import Optional

from trader.universe.lkg_store import load_lkg, save_lkg

logger = logging.getLogger(__name__)


class LKGProvider:
    def __init__(self, *, enabled: bool = True):
        self.enabled = enabled

    def load(self, env: str, strategy: str) -> Optional[dict]:
        if not self.enabled:
            logger.info("[UNIVERSE][LKG][DISABLED] env=%s strategy=%s", env, strategy)
            return None
        return load_lkg(env, strategy)

    def save(self, env: str, strategy: str, payload: dict) -> None:
        if not self.enabled:
            return
        save_lkg(env, strategy, payload)

