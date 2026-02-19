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
from trader.data.ohlcv_provider import (
    compute_required_prefetch_days,
    find_symbols_with_insufficient_history,
    upsert_ohlcv_delta,
)
from trader.exporter import export_watchlist_bundle
from trader.report.pdf_report import generate_watchlist_pdf
from trader.strategies.pb1_minervini_v2 import MinerviniConfig
from trader.time_utils import now_kst, resolve_derived_as_of
from trader.utils.json_sanitize import to_jsonable
from trader.universe.build import build_universe

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
    flow_mode = (os.getenv("FLOW_MODE", "PREV_CLOSE_ONLY") or "PREV_CLOSE_ONLY").strip().upper()
    flow_strict = _env_true("FLOW_STRICT", "0")
    if flow_mode not in {"PREV_CLOSE_ONLY", "PREV_DAY_ONLY"}:
        logger.warning("[FLOW][WARN] invalid FLOW_MODE=%s -> fallback=PREV_CLOSE_ONLY", flow_mode)
        flow_mode = "PREV_CLOSE_ONLY"

    logger.info(
        "[FLOW][SOURCE] mode=%s strict=%s foreign=%s inst=%s",
        flow_mode,
        int(flow_strict),
        foreign_src,
        inst_src,
    )

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
        # PREP에서는 전일(as_of) 확정치까지만 사용한다.
        try:
            foreign_df = _load_df(foreign_src, code, as_of, window)
            inst_df = _load_df(inst_src, code, as_of, window)
            if foreign_df is None or inst_df is None:
                logger.warning(
                    "[FLOW][WARN] missing flow symbol=%s as_of=%s foreign_missing=%s inst_missing=%s",
                    code,
                    as_of,
                    int(foreign_df is None),
                    int(inst_df is None),
                )
            return foreign_df, inst_df
        except Exception as exc:
            logger.warning("[FLOW][WARN] fetch failed symbol=%s as_of=%s err=%s", code, as_of, exc)
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
    Ensure universe exists in DB for (env, strategy, as_of) with strict as_of semantics.
    - Load with fallback disabled.
    - If missing (or force always), build and persist for exact as_of.
    - Reload exact as_of and fail hard when still empty.
    """
    repo = UniverseRepo(engine)
    as_of_s = as_of.isoformat()

    no_fallback = os.getenv("UNIVERSE_NO_FALLBACK", "1") == "1"
    build_if_missing = os.getenv("UNIVERSE_BUILD_IF_MISSING", "1") == "1"
    force_always = os.getenv("UNIVERSE_FORCE_REBUILD_ALWAYS", "0") == "1"

    # 1) strict as_of load (no fallback)
    members = repo.get_universe_members(
        env=env,
        strategy=strategy,
        as_of_date=as_of_s,
        allow_fallback=False,
    )

    # 2) build trigger
    should_build = force_always or len(members) == 0
    if should_build:
        if len(members) == 0 and not build_if_missing:
            raise RuntimeError(
                f"Universe missing and auto-build disabled: env={env} strategy={strategy} as_of={as_of_s}"
            )

        reason = "force_always" if force_always else "missing_as_of"
        logger.warning(
            "[UNIVERSE][AUTO_BUILD][RUN] env=%s strategy=%s as_of=%s reason=%s no_fallback=%s build_if_missing=%s",
            env,
            strategy,
            as_of_s,
            reason,
            no_fallback,
            build_if_missing,
        )
        built = build_universe(as_of_date=as_of_s, env=env, strategy=strategy)
        logger.info("[UNIVERSE][DB][UPSERT/SAVE] env=%s strategy=%s as_of=%s members=%d", env, strategy, as_of_s, len(built))
        logger.info("[UNIVERSE][AUTO_BUILD][DONE] as_of=%s members=%d", as_of_s, len(built))

        # 3) strict reload verification (exact as_of only)
        members = repo.get_universe_members(
            env=env,
            strategy=strategy,
            as_of_date=as_of_s,
            allow_fallback=False,
        )

    # 4) hard fail if exact as_of still empty
    if len(members) == 0:
        raise RuntimeError(
            f"Universe build/save failed: as_of universe is empty after ensure/build: "
            f"env={env} strategy={strategy} as_of={as_of_s}"
        )

    return members


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    assert_db_ready()
    engine = get_engine()
    run_migrations(engine)

    env = os.getenv("STRATEGY_ENV", "practice").lower()
    mode = os.getenv("MODE", "prep").strip().lower()
    universe_strategy = os.getenv("CANDIDATE_POOL_UNIVERSE_STRATEGY", "best_k_meta")
    as_of = _pick_as_of_date_always_prev()
    degraded_exclude_flow = _env_true("DEGRADED_EXCLUDE_FLOW", "1")

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

    t_ohlcv = time.monotonic()
    prefetch_mode = os.getenv("OHLCV_PREFETCH_MODE", "auto").strip().lower()
    prefetch_days = int(os.getenv("PREFETCH_DAYS", "5"))
    delta_days = max(1, int(os.getenv("OHLCV_DELTA_DAYS", "5")))
    auto_min_history_days = max(1, int(os.getenv("OHLCV_AUTO_MIN_HISTORY_DAYS", "150")))
    vcp_lookback = int(os.getenv("MINERVINI_BASE_LOOKBACK_MAX", "80"))
    rs_lookbacks = [
        int(os.getenv("RS_LOOKBACK_DAYS", "63")),
        int(os.getenv("RS_LOOKBACK2_DAYS", "126")),
    ]
    need_days = compute_required_prefetch_days(
        prefetch_days=prefetch_days,
        vcp_lookback=vcp_lookback,
        rs_lookbacks=rs_lookbacks,
    )

    logger.info(
        "[PREP][PHASE] ohlcv_prefetch_start symbols=%s mode=%s prefetch_days=%s delta_days=%s need_days=%s benchmark=%s",
        len(symbols),
        prefetch_mode,
        prefetch_days,
        delta_days,
        need_days,
        bench,
    )

    if prefetch_mode not in {"auto", "full", "delta"}:
        logger.warning("[PREP][OHLCV][MODE_INVALID] mode=%s -> fallback=auto", prefetch_mode)
        prefetch_mode = "auto"

    delta_result = {"symbols": 0, "inserted": 0, "updated": 0, "failed": 0, "failed_symbols": [], "dates": []}
    full_result = {"symbols": 0, "inserted": 0, "updated": 0, "failed": 0, "failed_symbols": [], "dates": []}

    if prefetch_mode == "delta":
        delta_result = upsert_ohlcv_delta(symbols=symbols, as_of=as_of, days=delta_days)
    elif prefetch_mode == "full":
        full_result = upsert_ohlcv_delta(symbols=symbols, as_of=as_of, days=int(need_days))
    else:
        delta_result = upsert_ohlcv_delta(symbols=symbols, as_of=as_of, days=delta_days)
        insufficient_symbols, _ = find_symbols_with_insufficient_history(
            symbols=symbols,
            as_of=as_of,
            min_history_days=auto_min_history_days,
        )
        logger.info(
            "[PREP][OHLCV][AUTO_CHECK] min_history_days=%s insufficient=%s/%s",
            auto_min_history_days,
            len(insufficient_symbols),
            len(symbols),
        )
        if insufficient_symbols:
            sample = sorted(insufficient_symbols)[:10]
            logger.warning(
                "[PREP][OHLCV][AUTO_BACKFILL] symbols=%s need_days=%s sample=%s",
                len(insufficient_symbols),
                need_days,
                sample,
            )
            full_result = upsert_ohlcv_delta(symbols=insufficient_symbols, as_of=as_of, days=int(need_days))
        else:
            logger.info("[PREP][OHLCV][AUTO_BACKFILL] skipped (all symbols have enough history)")

    logger.info(
        "[OHLCV][PREFETCH_DONE] mode=%s benchmark=%s delta=%s full=%s",
        prefetch_mode,
        bench,
        to_jsonable(delta_result),
        to_jsonable(full_result),
    )

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
    watchlist_force_rebuild = (
        force_candidate
        or _env_true("WATCHLIST_FORCE_REBUILD", "0")
        or _env_true("PB1_WATCHLIST_FORCE_REBUILD", "0")
        or mode == "minervini_test"
    )

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
        force_rebuild=bool(watchlist_force_rebuild),
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

    # watchlist 최종 검증 (30 미만이어도 계속 진행, 가능한 만큼 산출)
    finaln = int(os.getenv("PB1_WATCHLIST_FINALN", "30"))
    shortage_reason = ""
    if len(watchlist) < finaln:
        logger.warning(
            "[PREP][WATCHLIST][TOO_SMALL] got=%s expected=%s -> reload from DB (soft)",
            len(watchlist), finaln
        )
        watchlist_repo = WatchlistRepo(engine)
        rows, _used_as_of = watchlist_repo.load_watchlist(
            env=env,
            strategy=os.getenv("PB1_WATCHLIST_STRATEGY", "pb1_watchlist"),
            as_of=as_of
        )
        if rows:
            watchlist = rows
            watchlist_bundle["final30"] = rows
            watchlist_bundle["final_count"] = len(rows)
            if len(rows) >= finaln:
                logger.info("[PREP][WATCHLIST][FIXED_FROM_DB] count=%s", len(watchlist))
            else:
                shortage_reason = f"db_too_small:{len(rows)}<{finaln}"
                logger.warning(
                    "[PREP][WATCHLIST][DB_TOO_SMALL][SOFT] db_count=%s expected=%s -> continue",
                    len(rows),
                    finaln,
                )
        else:
            shortage_reason = f"db_missing:{as_of.isoformat()}"
            logger.warning("[PREP][WATCHLIST][MISSING][SOFT] env=%s as_of=%s -> continue", env, as_of)

    if not shortage_reason and len(watchlist) < finaln:
        shortage_reason = f"pipeline_shortage:{len(watchlist)}<{finaln}"

    watchlist_repo = WatchlistRepo(engine)
    watchlist_final_strategy = os.getenv("WATCHLIST_FINAL_STRATEGY_KEY", "pb1_watchlist_final").strip().lower()
    if watchlist:
        watchlist_repo.save_watchlist(
            env=env,
            strategy=watchlist_final_strategy,
            as_of=as_of,
            members=watchlist,
        )
        logger.info(
            "[PREP][WATCHLIST_FINAL][SAVE] strategy=%s as_of=%s n=%s",
            watchlist_final_strategy,
            as_of,
            len(watchlist),
        )

    run_id = os.getenv("TRADER_RUN_ID") or str(uuid4())
    os.environ["TRADER_RUN_ID"] = run_id
    ledger_repo = LedgerEventsRepo(engine)

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
            "expected_finaln": finaln,
            "final_count": int(watchlist_bundle.get("final_count", len(watchlist or []))),
            "shortage_reason": shortage_reason,
            "degrade": watchlist_bundle.get("degrade", {}),
        },
    )

    final_df = frames.get("final30", pd.DataFrame())
    if final_df is None or final_df.empty:
        logger.error("[PREP][DEGRADED] reason=empty_final30 as_of=%s", as_of)
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            strategy="pb1_pullback_close",
            run_window="prep",
            event_type="PREP_DEGRADED",
            ts=now_kst(),
            ok=False,
            reasons=["empty_final30"],
            payload_json={"as_of": as_of.isoformat(), "final_count": 0},
        )
        return 1

    core_metric_cols = ["rs_pctile", "vcp_score", "atr_pct", "trend_score", "pullback_pct"]
    flow_metric_cols = ["foreign_20_ratio", "inst_20_ratio", "flow_score"]

    flow_available_cols = [col for col in flow_metric_cols if col in final_df.columns]
    if flow_available_cols:
        flow_sample = final_df[flow_available_cols]
        flow_missing_mask = flow_sample.isna().all(axis=1) | flow_sample.fillna(0.0).eq(0.0).all(axis=1)
        flow_missing_ratio = float(flow_missing_mask.mean()) if len(flow_sample) > 0 else 1.0
        if flow_missing_ratio > 0:
            logger.warning(
                "[FLOW][WARN] missing_flow_rows=%s/%s ratio=%.3f as_of=%s (non-blocking)",
                int(flow_missing_mask.sum()),
                int(len(flow_sample)),
                flow_missing_ratio,
                as_of,
            )

    metric_cols = [col for col in core_metric_cols if col in final_df.columns]
    if not degraded_exclude_flow:
        metric_cols.extend(flow_available_cols)

    available_cols = metric_cols
    if available_cols:
        sample = final_df[available_cols].fillna(0.0)
        zero_ratio = float((sample == 0.0).all(axis=1).mean()) if len(sample) > 0 else 1.0
        if zero_ratio >= 0.8:
            logger.error(
                "[PREP][DEGRADED] reason=metrics_mostly_zero ratio=%.3f as_of=%s metric_scope=%s",
                zero_ratio,
                as_of,
                "core_only" if degraded_exclude_flow else "core_plus_flow",
            )
            ledger_repo.append_event(
                env=env,
                run_id=run_id,
                strategy="pb1_pullback_close",
                run_window="prep",
                event_type="PREP_DEGRADED",
                ts=now_kst(),
                ok=False,
                reasons=["metrics_mostly_zero"],
                payload_json={
                    "as_of": as_of.isoformat(),
                    "zero_ratio": zero_ratio,
                    "metric_cols": available_cols,
                    "metric_scope": "core_only" if degraded_exclude_flow else "core_plus_flow",
                    "final_count": int(len(sample)),
                },
            )
            return 1

    try:
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
    except Exception:
        logger.exception("[REPORT][WATCHLIST][PDF][FAIL] as_of=%s output_dir=%s", as_of, export_dir)
    payload = to_jsonable(
        {
            "as_of": as_of.isoformat(),
            "symbols": len(symbols),
            "ohlcv_prefetch_mode": prefetch_mode,
            "delta": delta_result,
            "full": full_result,
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
