from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta
from uuid import uuid4

import pandas as pd

from trader.db.engine import get_engine
from trader.db.health import assert_db_ready
from trader.db.migrate import run_migrations
from trader.db.repos import LedgerEventsRepo, UniverseRepo
from trader.minervini.compute import compute_and_store_derived_minervini
from trader.candidate_pool_builder import build_and_save_candidate_pool
from trader.watchlist_builder import build_and_save_watchlist
from trader.data.ohlcv_provider import upsert_ohlcv_delta
from trader.strategies.pb1_minervini_v2 import MinerviniConfig
from trader.time_utils import now_kst, prev_business_day
from trader.utils.json_sanitize import to_jsonable
from trader.config import RS_BENCHMARK

logger = logging.getLogger(__name__)


def _pick_as_of_date_always_prev() -> date:
    """PREP는 항상 전 거래일 기준으로 실행 (장전/장중/장후 무관)"""
    now = now_kst()
    return prev_business_day(now.date())


def _as_of_today() -> date:
    """Deprecated: 전일 고정을 위해 _pick_as_of_date_always_prev() 사용"""
    return now_kst().date()


def _load_db_ohlcv_df(*, engine, code: str, as_of: date, count: int) -> pd.DataFrame:
    from trader.db.repos import load_price_daily

    start_date = as_of - timedelta(days=count * 2)
    candles = load_price_daily(engine, code, start_date, as_of)
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def _ensure_universe(*, engine, env: str, strategy: str, as_of: date) -> list[dict]:
    repo = UniverseRepo(engine)
    members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of.isoformat())
    if members:
        return members

    from trader.universe import build as universe_build

    built = universe_build.build_universe(as_of_date=as_of.isoformat(), env=env, strategy=strategy)
    if built:
        repo.save_universe_run_and_members(env=env, strategy=strategy, as_of=as_of.isoformat(), members=built)
        members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of.isoformat())
    return members or []


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    assert_db_ready()
    engine = get_engine()
    run_migrations(engine)

    env = os.getenv("STRATEGY_ENV", "practice").lower()
    universe_strategy = os.getenv("CANDIDATE_POOL_UNIVERSE_STRATEGY", "best_k_meta")
    as_of = _pick_as_of_date_always_prev()

    logger.info("[PREP][START] env=%s as_of=%s (PREV_TRADING_DAY)", env, as_of)
    t0 = time.monotonic()

    members = _ensure_universe(engine=engine, env=env, strategy=universe_strategy, as_of=as_of)
    if not members:
        logger.error("[PREP][FAIL] universe empty")
        return 1

    symbols = [m.get("code") for m in members if m.get("code")]
    benchmark_symbols = [RS_BENCHMARK or "229200", "229200"]
    symbols_for_ohlcv = sorted({str(s).zfill(6) for s in (symbols + benchmark_symbols) if s})

    t_ohlcv = time.monotonic()
    delta_days = int(os.getenv("OHLCV_DELTA_DAYS", "1"))
    delta_result = upsert_ohlcv_delta(symbols=symbols_for_ohlcv, as_of=as_of, days=delta_days)
    dt_ohlcv = time.monotonic() - t_ohlcv

    t_derived = time.monotonic()
    derived_upserted = compute_and_store_derived_minervini(
        engine=engine,
        symbols=symbols,
        as_of=as_of,
        lookback_days=int(os.getenv("MINERVINI_OHLCV_DAYS", "520")),
    )
    dt_derived = time.monotonic() - t_derived

    t_pool = time.monotonic()
    force_candidate = os.getenv("FORCE_CANDIDATE", "0") == "1"

    def _pool_ohlcv(code: str, days: int = 80):
        return _load_db_ohlcv_df(engine=engine, code=code, as_of=as_of, count=days)

    pool_codes = build_and_save_candidate_pool(
        engine=engine,
        env=env,
        as_of=as_of,
        members=members,
        ohlcv_provider=_pool_ohlcv,
        force_rebuild=force_candidate,
        skip_prefetch=True,
    )
    dt_pool = time.monotonic() - t_pool

    t_watchlist = time.monotonic()

    def _watchlist_ohlcv(code: str, count: int = 120):
        df = _load_db_ohlcv_df(engine=engine, code=code, as_of=as_of, count=count)
        return df, {"source": "db"}

    minervini_cfg = MinerviniConfig(rs_min_percentile=float(os.getenv("RS_MIN_PCTILE", "80")) / 100.0)
    watchlist = build_and_save_watchlist(
        engine=engine,
        env=env,
        strategy=os.getenv("PB1_WATCHLIST_STRATEGY", "pb1_watchlist"),
        as_of=as_of,
        members=members,
        ohlcv_provider=_watchlist_ohlcv,
        minervini_config={
            "rs_min_pctile": minervini_cfg.rs_min_percentile,
            "vcp_min_score": float(os.getenv("VCP_MIN_SCORE", "70")),
        },
        force_rebuild=bool(force_candidate),
    )
    dt_watchlist = time.monotonic() - t_watchlist

    run_id = os.getenv("TRADER_RUN_ID") or str(uuid4())
    os.environ["TRADER_RUN_ID"] = run_id
    ledger_repo = LedgerEventsRepo(engine)
    payload = to_jsonable(
        {
            "as_of": as_of.isoformat(),
            "symbols": len(symbols),
            "delta": delta_result,
            "derived_upserted": derived_upserted,
            "pool_size": len(pool_codes or []),
            "watchlist_size": len(watchlist or []),
            "durations_sec": {
                "ohlcv_delta": round(dt_ohlcv, 2),
                "derived": round(dt_derived, 2),
                "candidate_pool": round(dt_pool, 2),
                "watchlist": round(dt_watchlist, 2),
                "total": round(time.monotonic() - t0, 2),
            },
        }
    )
    ledger_repo.append_event(
        env=env,
        run_id=run_id,
        strategy="pb1_pullback_close",
        run_window="prep",
        event_type="PREP_DONE",
        ts=now_kst(),
        payload_json=payload,
    )
    logger.info(
        "[LEDGER_EVENT] event_type=PREP_DONE as_of=%s symbols=%s",
        as_of.isoformat(),
        len(symbols),
    )

    logger.info(
        "[PREP][DONE] as_of=%s symbols=%s pool=%s watchlist=%s dt=%.2f",
        as_of,
        len(symbols),
        len(pool_codes or []),
        len(watchlist or []),
        time.monotonic() - t0,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
