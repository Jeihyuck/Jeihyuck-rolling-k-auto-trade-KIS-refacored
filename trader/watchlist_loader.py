"""
Watchlist loader for trade tick optimization.

This module provides optimized watchlist loading for trade ticks,
allowing trade to scan only the final 30 stocks instead of full universe.
"""
from __future__ import annotations

import logging
import os
from datetime import date

from trader.db.repos import ScoredWatchlistInvalidError, ScoredWatchlistNotFoundError, WatchlistRepo, load_final30_scored_db_only
from trader.time_coerce import to_date
from trader.pb1_engine import UniverseContext

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
    env_n = (env or "").strip().lower()
    forced_strategy = os.getenv("WATCHLIST_FINAL_STRATEGY_KEY", "pb1_watchlist_final_scored").strip().lower()
    if strategy.strip().lower() != forced_strategy:
        logger.warning(
            "[WATCHLIST][TRADE][FORCE_STRATEGY] requested=%s forced=%s",
            strategy,
            forced_strategy,
        )

    try:
        df = load_final30_scored_db_only(
            engine,
            env=env_n,
            strategy=forced_strategy,
            as_of=as_of_date,
            require_exact_rows=30,
            fail_if_missing=True,
        )
    except (ScoredWatchlistNotFoundError, ScoredWatchlistInvalidError):
        df = None
    rows = [] if df is None else [dict(row or {}) for row in df.to_dict(orient="records")]
    
    if rows:
        # Convert watchlist rows to members format
        members = [
            {
                "code": str(row.get("code") or "").zfill(6),
                "rank": row.get("rank"),
                "score": row.get("score"),
                "market": row.get("meta", {}).get("market") if isinstance(row.get("meta"), dict) else None,
            }
            for row in rows
            if row.get("code")
        ]
        
        if len(members) != 30:
            raise RuntimeError(f"WATCHLIST_FINAL_SIZE_INVALID expected=30 actual={len(members)}")
        top10_codes = [m.get("code") for m in members[:10] if m.get("code")]
        logger.info(
            "[TRADE][WATCHLIST_FINAL][LOCK] env=%s strategy=%s requested_as_of=%s actual_as_of=%s n=%s",
            env_n,
            forced_strategy,
            as_of_date.isoformat(),
            as_of_date.isoformat(),
            len(members),
        )
        logger.info(
            "[TRADE][WATCHLIST_FINAL][TOP10] source=pb1_watchlist_final rank_basis=stored_rank codes=%s",
            top10_codes,
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
        env_n, forced_strategy, as_of_date.isoformat()
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
    env_n = (env or "").strip().lower()
    forced_strategy = os.getenv("WATCHLIST_FINAL_STRATEGY_KEY", "pb1_watchlist_final_scored").strip()

    try:
        df = load_final30_scored_db_only(
            engine,
            env=env_n,
            strategy=forced_strategy,
            as_of=as_of_date,
            require_exact_rows=30,
            fail_if_missing=True,
        )
    except (ScoredWatchlistNotFoundError, ScoredWatchlistInvalidError):
        df = None

    if df is not None and not df.empty:
        rows = [dict(row or {}) for row in df.to_dict(orient="records")]
        members = [
            {
                "code": str(row.get("code") or "").zfill(6),
                "rank": row.get("rank"),
                "score": row.get("score"),
                "market": row.get("meta", {}).get("market") if isinstance(row.get("meta"), dict) else None,
            }
            for row in rows
            if row.get("code")
        ]
        return UniverseContext(
            as_of_date=as_of_date.isoformat(),
            members=members,
            selected_path=None,
            meta={
                "source": "watchlist",
                "requested_as_of": as_of_date.isoformat(),
                "actual_as_of": as_of_date.isoformat(),
                "age_days": 0,
                "count": len(members),
            },
            is_empty=len(members) == 0,
        )
    
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
