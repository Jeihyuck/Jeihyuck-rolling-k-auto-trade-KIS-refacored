from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd
import sqlalchemy as sa

from trader.db.engine import get_engine
from trader.db.health import assert_db_ready
from trader.db.migrate import run_migrations
from trader.db.repos import LedgerEventsRepo, UniverseRepo, WatchlistRepo
from trader.minervini.compute import compute_and_store_derived_minervini
from trader.candidate_pool_builder import build_and_save_candidate_pool
from trader.watchlist_builder import build_and_save_watchlist
from trader.data.ohlcv_provider import upsert_ohlcv_delta
from trader.exporter import export_watchlist_bundle
from trader.report.pdf_report import generate_watchlist_pdf
from trader.strategies.pb1_minervini_v2 import MinerviniConfig
from trader.time_utils import now_kst, resolve_derived_as_of
from trader.utils.json_sanitize import to_jsonable
from trader.universe.build import build_universe
from trader.config import EMERGENCY_UNIVERSE_BUILD, FORCE_UNIVERSE_REBUILD

logger = logging.getLogger(__name__)


def _env_true(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def _detect_flow_sources(engine) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    """DB에 존재하는 flow 테이블/컬럼을 탐색한다."""
    table_candidates = [
        "investor_flow_daily",
        "investor_trading_daily",
        "investor_daily",
        "flow_daily",
        "stock_investor_daily",
    ]
    date_cols = ["date", "as_of", "trade_date", "dt"]
    code_cols = ["code", "symbol", "stock_code"]
    foreign_cols = ["foreign_net_buy", "foreign_net_qty", "frgn_ntby_qty", "foreigner_net_buy"]
    inst_cols = ["inst_net_buy", "institution_net_buy", "institutional_net_buy", "orgn_ntby_qty"]

    insp = sa.inspect(engine)
    tables = set(insp.get_table_names())
    foreign_src = None
    inst_src = None

    for table in table_candidates:
        if table not in tables:
            continue
        cols = {c["name"] for c in insp.get_columns(table)}
        date_col = next((c for c in date_cols if c in cols), None)
        code_col = next((c for c in code_cols if c in cols), None)
        if not date_col or not code_col:
            continue
        if foreign_src is None:
            flow_col = next((c for c in foreign_cols if c in cols), None)
            if flow_col:
                foreign_src = {"table": table, "date_col": date_col, "code_col": code_col, "flow_col": flow_col}
        if inst_src is None:
            flow_col = next((c for c in inst_cols if c in cols), None)
            if flow_col:
                inst_src = {"table": table, "date_col": date_col, "code_col": code_col, "flow_col": flow_col}
        if foreign_src and inst_src:
            break

    return foreign_src, inst_src


def _make_flow_provider(engine):
    foreign_src, inst_src = _detect_flow_sources(engine)
    logger.info("[FLOW][SOURCE] foreign=%s inst=%s", foreign_src, inst_src)

    def _load_df(source: dict[str, str] | None, code: str, as_of: date, window: int) -> pd.DataFrame | None:
        if source is None:
            return None
        stmt = sa.text(
            f"""
            SELECT {source['date_col']} AS date, {source['flow_col']} AS net_buy
            FROM {source['table']}
            WHERE {source['code_col']} = :code
              AND {source['date_col']} <= :as_of
            ORDER BY {source['date_col']} DESC
            LIMIT :lim
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(stmt, {"code": code, "as_of": as_of, "lim": int(max(window, 20) * 2)}).fetchall()
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["date", "net_buy"])
        if df.empty:
            return None
        df = df.sort_values("date")
        return df

    def _provider(code: str, as_of: date, window: int):
        try:
            return _load_df(foreign_src, code, as_of, window), _load_df(inst_src, code, as_of, window)
        except Exception as exc:
            logger.debug("[FLOW][LOAD][FAIL] code=%s err=%s", code, exc)
            return None, None

    return _provider


def _pick_as_of_date_always_prev() -> date:
    """PREP as_of 결정: AS_OF_OVERRIDE 우선, 없으면 전 거래일."""
    return resolve_derived_as_of(now_kst())


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
    """
    Ensure universe exists in DB for (env, strategy, as_of).
    If missing and emergency_build/force_rebuild enabled -> build + save + reload.
    If still missing -> fail hard (no silent fallback).
    """
    repo = UniverseRepo(engine)
    as_of_s = as_of.isoformat()

    # 1) load
    members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of_s)

    # 2) decide build
    force_always = os.getenv("UNIVERSE_FORCE_REBUILD_ALWAYS", "0") == "1"

    should_build = False
    reason = ""

    if len(members) == 0 and EMERGENCY_UNIVERSE_BUILD:
        should_build = True
        reason = "emergency_missing"
    elif FORCE_UNIVERSE_REBUILD:
        if force_always:
            should_build = True
            reason = "force_always"
        elif len(members) == 0:
            should_build = True
            reason = "force_missing"
        else:
            # 안전장치: 이미 있으면 강제 재빌드 스킵
            logger.info(
                "[UNIVERSE][AUTO_BUILD][SKIP] universe already exists (members=%d). "
                "Set UNIVERSE_FORCE_REBUILD_ALWAYS=1 to rebuild anyway.",
                len(members),
            )
            should_build = False

    if should_build:
        logger.warning(
            "[UNIVERSE][AUTO_BUILD] trigger build: env=%s strategy=%s as_of=%s (members=%d) reason=%s emergency=%s force=%s",
            env, strategy, as_of_s, len(members), reason, EMERGENCY_UNIVERSE_BUILD, FORCE_UNIVERSE_REBUILD
        )
        built = build_universe(as_of_date=as_of_s, env=env, strategy=strategy)
        logger.info("[UNIVERSE][AUTO_BUILD] built_members=%d (saved by builder)", len(built))
        members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of_s)

    # 5) hard fail if still empty
    if len(members) == 0:
        raise RuntimeError(
            f"Universe empty after ensure/build: env={env} strategy={strategy} as_of={as_of_s} "
            f"(emergency={EMERGENCY_UNIVERSE_BUILD}, force={FORCE_UNIVERSE_REBUILD})"
        )

    return members


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    assert_db_ready()
    engine = get_engine()
    run_migrations(engine)

    env = os.getenv("STRATEGY_ENV", "practice").lower()
    universe_strategy = os.getenv("CANDIDATE_POOL_UNIVERSE_STRATEGY", "best_k_meta")
    as_of = _pick_as_of_date_always_prev()

    as_of_reason = "AS_OF_OVERRIDE" if (os.getenv("AS_OF_OVERRIDE") or "").strip() else "PREV_TRADING_DAY"
    logger.info("[PREP][START] env=%s as_of=%s (%s)", env, as_of, as_of_reason)
    t0 = time.monotonic()

    members = _ensure_universe(engine=engine, env=env, strategy=universe_strategy, as_of=as_of)
    if not members:
        logger.error("[PREP][FAIL] universe empty")
        return 1

    # ---- RS benchmark handling (229200 etc.) ----
    bench = os.getenv("RS_BENCHMARK", "229200").strip()

    # Universe symbols
    symbols = [m.get("code") for m in members if m.get("code")]

    # Ensure benchmark included for downstream RS/Stage_B computations
    if bench and bench not in symbols:
        symbols.append(bench)

    # Use a longer backfill window for benchmark so RS lookbacks never fail.
    # RS rank requires ~127 trading days; use 260/520 to be safe.
    bench_days = int(os.getenv("BENCH_OHLCV_DAYS", "520"))

    t_ohlcv = time.monotonic()
    delta_days = int(os.getenv("OHLCV_DELTA_DAYS", "1"))

    # 1) universe + bench: recent delta
    delta_result = upsert_ohlcv_delta(symbols=symbols, as_of=as_of, days=delta_days)

    # 2) benchmark: long backfill (only for bench) to satisfy RS required window
    if bench and bench_days > delta_days:
        upsert_ohlcv_delta(symbols=[bench], as_of=as_of, days=bench_days)

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

    flow_provider = _make_flow_provider(engine)

    minervini_cfg = MinerviniConfig(rs_min_percentile=float(os.getenv("RS_MIN_PCTILE", "80")) / 100.0)
    watchlist_result = build_and_save_watchlist(
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
        flow_provider=flow_provider,
        return_bundle=True,
    )
    if isinstance(watchlist_result, tuple):
        watchlist, watchlist_bundle = watchlist_result
    else:
        watchlist = watchlist_result
        watchlist_bundle = {
            "as_of": as_of,
            "weights": {
                "tech_weight": float(os.getenv("WATCHLIST_TECH_WEIGHT", "0.7")),
                "flow_weight": float(os.getenv("WATCHLIST_FLOW_WEIGHT", "0.3")),
            },
            "universe_scored": [],
            "pool120": [],
            "top50": [],
            "final30": watchlist,
            "reject_summary": {},
        }
    dt_watchlist = time.monotonic() - t_watchlist

    export_dir = Path("runtime/watchlist") / as_of.strftime("%Y-%m-%d")
    frames = {
        "universe_scored": pd.DataFrame(watchlist_bundle.get("universe_scored", [])),
        "pool120": pd.DataFrame(watchlist_bundle.get("pool120", [])),
        "top50": pd.DataFrame(watchlist_bundle.get("top50", [])),
        "final30": pd.DataFrame(watchlist_bundle.get("final30", watchlist or [])),
    }
    export_watchlist_bundle(
        out_dir=export_dir,
        frames_dict=frames,
        meta_dict={
            "as_of": as_of.isoformat(),
            "env": env,
            "weights": watchlist_bundle.get("weights", {}),
            "reject_summary": watchlist_bundle.get("reject_summary", {}),
        },
    )
    pdf_path = generate_watchlist_pdf(
        as_of=as_of,
        output_dir=export_dir,
        pool120=watchlist_bundle.get("pool120", []),
        top50=watchlist_bundle.get("top50", []),
        final30=watchlist_bundle.get("final30", watchlist or []),
        reject_summary=watchlist_bundle.get("reject_summary", {}),
        weights=watchlist_bundle.get("weights", {}),
    )
    logger.info("[PDF] wrote %s", pdf_path)

    # 보험: watchlist 최종 검증 (30 미만이면 DB에서 재확정)
    finaln = int(os.getenv("PB1_WATCHLIST_FINALN", "30"))
    mode = str(os.getenv("MODE", "")).strip().lower()
    dryrun = _env_true("DRYRUN") or _env_true("DRY_RUN")
    analysis_only = _env_true("ANALYSIS_ONLY")
    strict_watchlist_min = _env_true("STRICT_WATCHLIST_MIN") and mode == "trade" and not dryrun and not analysis_only
    soft_mode = mode == "minervini_test" or dryrun or analysis_only
    if len(watchlist) < finaln:
        logger.warning(
            "[PREP][WATCHLIST][TOO_SMALL] got=%s expected=%s -> reload exact from DB",
            len(watchlist), finaln
        )
        watchlist_repo = WatchlistRepo(engine)
        rows, _used_as_of = watchlist_repo.load_watchlist(
            env=env,
            strategy=os.getenv("PB1_WATCHLIST_STRATEGY", "pb1_watchlist"),
            as_of=as_of
        )
        if rows and len(rows) >= finaln:
            watchlist = rows
            logger.info("[PREP][WATCHLIST][FIXED_FROM_DB] count=%s", len(watchlist))
        elif rows:
            logger.warning(
                "[PREP][WATCHLIST][DB_TOO_SMALL] db_count=%s < expected=%s mode=%s dryrun=%s analysis_only=%s strict=%s",
                len(rows), finaln, mode, dryrun, analysis_only, strict_watchlist_min
            )
            watchlist = rows
            if strict_watchlist_min and not soft_mode:
                raise RuntimeError(f"watchlist too small even in DB: {len(rows)} < {finaln}")
        else:
            msg = f"watchlist missing in DB: env={env} as_of={as_of}"
            if strict_watchlist_min and not soft_mode:
                raise RuntimeError(msg)
            logger.warning("[PREP][WATCHLIST][MISSING][SOFT] %s", msg)

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
