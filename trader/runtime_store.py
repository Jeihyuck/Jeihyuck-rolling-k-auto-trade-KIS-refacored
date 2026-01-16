from __future__ import annotations

import logging
import os
from datetime import date

from trader.db.engine import make_engine
from trader.db.repos import UniverseRepo
from trader.universe.lkg_store import lkg_path

logger = logging.getLogger(__name__)

DEFAULT_UNIVERSE_STRATEGY = "best_k_meta"


def universe_today_exists(today: date) -> bool:
    env = (os.getenv("KIS_ENV") or "practice").lower()
    strategy = os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
    as_of = today.isoformat()
    engine = make_engine()
    repo = UniverseRepo(engine)
    members = repo.get_universe_members(env, strategy, as_of)
    db_ok = bool(members)
    lkg_ok = lkg_path(env, strategy).exists()
    ok = db_ok and lkg_ok
    logger.info(
        "[UNIVERSE][CHECK] date=%s db=%s lkg=%s -> ok=%s",
        as_of,
        "Y" if db_ok else "N",
        "Y" if lkg_ok else "N",
        "Y" if ok else "N",
    )
    return ok
