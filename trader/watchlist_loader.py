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
from trader.time_utils import prev_business_day

logger = logging.getLogger(__name__)


def load_watchlist_for_trade(
    *,
    engine,
    env: str,
    strategy: str,
    as_of: str | date,
    fallback_to_universe: bool = False,
) -> UniverseContext:
    """
    Load watchlist for trade tick (optimized for latency).
    
    CRITICAL: Trade should scan ONLY watchlist (final 30), not full universe.
    
    Args:
        engine: SQLAlchemy engine
        env: Account environment (practice/paper/real)
        strategy: Strategy name
        as_of: Date to load watchlist for
        fallback_to_universe: Deprecated. Universe fallback is forbidden.
    
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
    
    # Watchlist empty
    logger.warning(
        "[WATCHLIST][LOAD][EMPTY] env=%s strategy=%s as_of=%s",
        env, strategy, as_of_date.isoformat()
    )

    return UniverseContext(
        as_of_date=as_of_date.isoformat(),
        members=[],
        selected_path=None,
        meta={
            "source": "watchlist",
            "as_of": as_of_date.isoformat(),
            "count": 0,
            "warning": "watchlist_missing",
        },
        is_empty=True,
    )


def load_today_watchlist_with_fallback(
    *,
    engine,
    env: str,
    strategy: str,
    as_of: str | date,
    ttl_days: int = 7,
    max_back_days: int = 3,
) -> UniverseContext:
    """
    Load today's watchlist with intelligent fallback.
    
    Strategy:
    1. Try as_of date
    2. If empty, try previous business day up to max_back_days
    3. Reject if no result within ttl_days
    
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
    
    # Find watchlist by previous business-day backtracking
    fallback_date = as_of_date
    for _ in range(max_back_days):
        fallback_date = prev_business_day(fallback_date)
        days_back = (as_of_date - fallback_date).days
        if days_back > ttl_days:
            break
        
        ctx_fallback = load_watchlist_for_trade(
            engine=engine,
            env=env,
            strategy=strategy,
            as_of=fallback_date,
            fallback_to_universe=False,
        )
        
        if not ctx_fallback.is_empty:
            logger.warning(
                "[WATCHLIST][FALLBACK][BIZDAY_OK] requested=%s actual=%s age=%d members=%s",
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
    
    # No watchlist found within allowed backtracking/TTL window
    logger.error(
        "[WATCHLIST][FALLBACK][FAIL] requested=%s ttl=%d max_back_days=%d",
        as_of_date.isoformat(),
        ttl_days,
        max_back_days,
    )

    return UniverseContext(
        as_of_date=as_of_date.isoformat(),
        members=[],
        selected_path=None,
        meta={
            "source": "watchlist",
            "as_of": as_of_date.isoformat(),
            "count": 0,
            "warning": "fallback_not_found",
        },
        is_empty=True,
    )
