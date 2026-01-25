from __future__ import annotations

import argparse
import logging
import os
from datetime import date

from trader.universe import build as universe_build

logger = logging.getLogger(__name__)


def build_universe_best_k_meta(
    *,
    as_of: date,
    env: str,
    universe_pool: int,
    **kwargs,
) -> list[dict]:
    """Build a universe snapshot and return members for best_k_meta."""
    _ = universe_pool
    return universe_build.build_universe(
        as_of_date=as_of.isoformat(),
        env=env,
        strategy=kwargs.get("strategy") or "best_k_meta",
        provider_override=kwargs.get("provider_override"),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build universe snapshots and persist outputs")
    parser.add_argument("--env", default=os.getenv("KIS_ENV") or "practice")
    parser.add_argument("--strategy", default="best_k_meta")
    parser.add_argument("--as-of", dest="as_of", default=None)
    parser.add_argument("--provider", default=None)
    parser.add_argument("--save-lkg", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    as_of = args.as_of or date.today().isoformat()
    members = universe_build.build_universe(
        as_of_date=as_of,
        env=args.env,
        strategy=args.strategy,
        provider_override=args.provider,
    )
    logger.info("[UNIVERSE][BUILDER] as_of=%s members=%s", as_of, len(members))


if __name__ == "__main__":
    main()
