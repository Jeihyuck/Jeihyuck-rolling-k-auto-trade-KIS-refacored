"""
Watchlist loader for trade tick optimization.

This module provides optimized watchlist loading for trade ticks,
allowing trade to scan only the final 30 stocks instead of full universe.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from trader.db.repos import WatchlistRepo
from trader.time_coerce import to_date
from trader.pb1_engine import UniverseContext

logger = logging.getLogger(__name__)


def load_watchlist_for_trade(
    *,
    engine,
    env: str,
    strategy: str,
    as_of: str | date,
    fallback_to_universe: bool = True,
) -> UniverseContext:
    """
    Load watchlist for trade tick (optimized for latency).
    
    CRITICAL: Trade should scan ONLY watchlist (final 30), not full universe.
    Universe is used only for fallback when watchlist is missing.
    
    Args:
        engine: SQLAlchemy engine
        env: Account environment (practice/paper/real)
        strategy: Strategy name
        as_of: Date to load watchlist for
        fallback_to_universe: If True, fall back to universe if watchlist is empty
    
    Returns:
        UniverseContext with watchlist members
    
    Examples:
        >>> ctx = load_watchlist_for_trade(
        ...     engine=engine,
        ...     env="practice",
        ...     strategy="best_k_meta",
        ...     as_of="2026-02-13"
        ... )
        >>> len(ctx.members)  # Should be ~30
        30
    """
    as_of_date = to_date(as_of)
    repo = WatchlistRepo(engine)
    
    # Load watchlist (final 30)
    watchlist_rows = repo.load_watchlist(
        env=env,
        strategy=strategy,
        as_of=as_of_date,
    )
    
    if watchlist_rows:
        # Convert watchlist rows to members format
        members = [
            {
                "code": str(row.get("code") or "").zfill(6),
                "rank": row.get("rank"),
                "score": row.get("score"),
                "market": row.get("meta", {}).get("market") if isinstance(row.get("meta"), dict) else None,
            }
            for row in watchlist_rows
            if row.get("code")
        ]
        
        logger.info(
            "[WATCHLIST][LOAD][SUCCESS] env=%s strategy=%s as_of=%s members=%s source=watchlist",
            env, strategy, as_of_date.isoformat(), len(members)
        )
        
        return UniverseContext(
            as_of_date=as_of_date.isoformat(),
            members=members,
            selected_path=None,
            meta={
                "source": "watchlist",
                "as_of": as_of_date.isoformat(),
                "count": len(members),
            },
            is_empty=len(members) == 0,
        )
    
    # Watchlist empty - try fallback
    logger.warning(
        "[WATCHLIST][LOAD][EMPTY] env=%s strategy=%s as_of=%s -> fallback=%s",
        env, strategy, as_of_date.isoformat(), fallback_to_universe
    )
    
    if not fallback_to_universe:
        return UniverseContext(
            as_of_date=as_of_date.isoformat(),
            members=[],
            selected_path=None,
            meta={"source": "watchlist", "as_of": as_of_date.isoformat(), "count": 0},
            is_empty=True,
        )
    
    # Fallback to universe (NOT RECOMMENDED for latency)
    from trader.db.repos import UniverseRepo
    
    universe_repo = UniverseRepo(engine)
    universe_members = universe_repo.get_universe_members(
        env=env,
        strategy=strategy,
        as_of_date=as_of_date.isoformat(),
    )
    
    logger.warning(
        "[WATCHLIST][LOAD][FALLBACK_UNIVERSE] env=%s strategy=%s as_of=%s members=%s (SLOW)",
        env, strategy, as_of_date.isoformat(), len(universe_members)
    )
    
    return UniverseContext(
        as_of_date=as_of_date.isoformat(),
        members=universe_members,
        selected_path=None,
        meta={
            "source": "universe_fallback",
            "as_of": as_of_date.isoformat(),
            "count": len(universe_members),
            "warning": "watchlist_missing"
        },
        is_empty=len(universe_members) == 0,
    )


def load_today_watchlist_with_fallback(
    *,
    engine,
    env: str,
    strategy: str,
    as_of: str | date,
    ttl_days: int = 7,
) -> UniverseContext:
    """
    Load today's watchlist with intelligent fallback.
    
    Strategy:
    1. Try as_of date
    2. If empty, try most recent within TTL days
    3. If still empty, fall back to universe (with warning)
    
    Args:
        engine: SQLAlchemy engine
        env: Account environment
        strategy: Strategy name
        as_of: Target date
        ttl_days: Max days to look back for watchlist (default 7)
    
    Returns:
        UniverseContext with members
    """
    as_of_date = to_date(as_of)
    repo = WatchlistRepo(engine)
    
    # Try exact as_of
    ctx = load_watchlist_for_trade(
        engine=engine,
        env=env,
        strategy=strategy,
        as_of=as_of_date,
        fallback_to_universe=False,
    )
    
    if not ctx.is_empty:
        return ctx
    
    # Find most recent watchlist within TTL
    from datetime import timedelta
    
    for days_back in range(1, ttl_days + 1):
        fallback_date = as_of_date - timedelta(days=days_back)
        
        ctx_fallback = load_watchlist_for_trade(
            engine=engine,
            env=env,
            strategy=strategy,
            as_of=fallback_date,
            fallback_to_universe=False,
        )
        
        if not ctx_fallback.is_empty:
            logger.warning(
                "[WATCHLIST][FALLBACK][TTL_OK] requested=%s actual=%s age=%d members=%s",
                as_of_date.isoformat(),
                fallback_date.isoformat(),
                days_back,
                len(ctx_fallback.members),
            )
            # Update as_of_date in meta but keep actual date in log
            ctx_fallback.meta["requested_as_of"] = as_of_date.isoformat()
            ctx_fallback.meta["actual_as_of"] = fallback_date.isoformat()
            ctx_fallback.meta["age_days"] = days_back
            return ctx_fallback
    
    # No watchlist found within TTL - fallback to universe
    logger.error(
        "[WATCHLIST][FALLBACK][TTL_EXCEEDED] requested=%s ttl=%d -> using universe (SLOW)",
        as_of_date.isoformat(),
        ttl_days,
    )
    
    return load_watchlist_for_trade(
        engine=engine,
        env=env,
        strategy=strategy,
        as_of=as_of_date,
        fallback_to_universe=True,  # Last resort
    )
