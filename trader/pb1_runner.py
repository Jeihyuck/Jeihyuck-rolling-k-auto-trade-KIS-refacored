from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import traceback
import time as time_mod
import copy
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from trader.config import (
    AFTERNOON_WINDOW_END,
    AFTERNOON_WINDOW_START,
    CLOSE_AUCTION_END,
    CLOSE_AUCTION_START,
    DIAGNOSTIC_MODE,
    DIAGNOSTIC_ONLY,
    LEDGER_BASE_DIR,
    LEDGER_LOOKBACK_DAYS,
    MARKET_CLOSE_HHMM,
    MARKET_OPEN_HHMM,
    MORNING_EXIT_END,
    MORNING_EXIT_START,
    MORNING_WINDOW_END,
    MORNING_WINDOW_START,
    NONTRADING_SMOKE_DB_STORE,
    NONTRADING_SMOKE_FORCE,
    NONTRADING_SMOKE_FORCE_REBUILD,
    NONTRADING_SMOKE_TIMEOUT_SEC,
    PB1_ALLOW_PREOPEN_ENTRY,
    PB1_PREOPEN_END,
    PB1_PREOPEN_MAX_NEW_POSITIONS,
    PB1_PREOPEN_REQUIRE_BALANCE,
    PB1_PREOPEN_START,
    PB1_ENTRY_ENABLED,
    PB1_ENTRY_WINDOW_END,
    PB1_EXIT_WINDOW_END,
    PB1_MORNING_WINDOW_END,
    PB1_REQUIRE_BALANCE_FOR_ENTRY,
    PB1_FORCE_ENTRY_ON_PUSH,
    PB1_MAX_WAIT_FOR_WINDOW_MIN,
    PB1_WAIT_FOR_WINDOW,
    PAPER_MAX_CAPITAL_KRW,
    PAPER_RESET_AUTO_PURGE,
    PAPER_RESET_EVENT_ONLY_IN_PRACTICE,
    resolve_strategy_mode,
)
from trader.runtime_paths import runtime_root, runtime_path
from trader.db.engine import make_engine
from trader.db.health import assert_db_ready
from trader.db.locks import acquire_advisory_lock, release_advisory_lock
from trader.db.migrate import run_migrations
from trader.db.repos import (
    FillsRepo,
    LedgerEventsRepo,
    OrdersRepo,
    PositionsRepo,
    ReconcileLogRepo,
    RunsRepo,
    UniverseRepo,
)
from trader.diagnostics.nontrading_smoke import (
    nontrading_smoke_flag_path,
    run_nontrading_smoke_once,
    write_nontrading_smoke_flag,
)
from trader.kis_wrapper import KisAPI, KisBalanceUnavailable, KisTemporaryError
from trader.pb1_engine import PB1Engine, UniverseContext, resolve_pb1_phase
from trader.reconcile_kis import reconcile_kis, reconcile_today
from trader.reconcile_db import close_stale_positions
from trader.run_context import RunContext
from trader.universe.build import build_universe
from trader.universe.mode import is_db_only_mode
from trader.time_utils import calc_market_window_kst, is_trading_weekday, now_kst, week_monday, is_market_open_kst, market_close_dt_kst
from trader.utils.env import env_bool, parse_env_flag, resolve_mode
from trader.window_router import WindowDecision, decide_window
from trader.watchlist_builder import build_and_save_watchlist
from trader.db.repos import WatchlistRepo
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.strategies.pb1_minervini_v2 import MinerviniConfig

logger = logging.getLogger(__name__)
log = logger


def resolve_auto_strategy_mode(mode_env: str) -> str:
    """
    AUTO 모드 결정: 시간 기반으로 LIVE/DIAG 결정.
    
    Args:
        mode_env: STRATEGY_MODE 환경변수 값
    
    Returns:
        "LIVE" or "DIAG"
    
    정책:
        - mode_env가 "LIVE" 또는 "DIAG"이면 그대로 사용
        - mode_env가 "AUTO"이면:
            - 장중(09:00~15:20, 월~금): "LIVE"
            - 장외(주말, 장시작 전, 장마감 후): "DIAG"
    """
    mode_env = (mode_env or "").strip().upper()
    if mode_env in ("LIVE", "DIAG"):
        logger.info("[AUTO_MODE] mode=%s (forced)", mode_env)
        return mode_env
    
    # AUTO 모드: 시간 기반 결정
    is_market_open = is_market_open_kst(now_kst())
    resolved_mode = "LIVE" if is_market_open else "DIAG"
    logger.info(
        "[AUTO_MODE] mode=AUTO resolved=%s is_market_open=%s now=%s",
        resolved_mode,
        is_market_open,
        now_kst().strftime("%Y-%m-%d %H:%M:%S"),
    )
    return resolved_mode


def compute_loop_deadline(now: datetime) -> datetime:
    """
    루프 종료 시각 계산: 장 마감 - grace_min.
    
    Args:
        now: 현재 KST 시각
    
    Returns:
        루프 종료 시각 (KST)
    """
    grace_min = int(os.getenv("PB1_LOOP_GRACE_MIN", "3"))
    market_close = market_close_dt_kst(now)
    deadline = market_close - timedelta(minutes=grace_min)
    logger.info(
        "[LOOP_DEADLINE] market_close=%s grace_min=%s deadline=%s",
        market_close.strftime("%H:%M:%S"),
        grace_min,
        deadline.strftime("%H:%M:%S"),
    )
    return deadline

_WINDOW_MISMATCH_LOGGED = False
BALANCE_STATE_OK = "OK"
BALANCE_STATE_STALE_OK = "STALE_OK"
BALANCE_STATE_UNKNOWN = "UNKNOWN"
DEFAULT_UNIVERSE_STRATEGY = "best_k_meta"


def _run_build_watchlist_job() -> int:
    """
    ✅ 설계 1: 주 1회 워치리스트 빌드 JOB.
    
    - 유니버스 로드
    - Minervini 필터 적용
    - week_monday(today) 키로 DB에 저장
    - 주문 없이 종료
    """
    logger.info("[WATCHLIST][BUILD_JOB] START")
    
    try:
        from datetime import date
        
        # DB 준비
        assert_db_ready()
        engine = make_engine()
        run_migrations(engine)
        
        # 환경변수
        env = os.getenv("ENV", "live")
        strategy = os.getenv("STRATEGY", "best_k_meta")
        
        # 주간 키 계산
        today = now_kst().date()
        as_of = week_monday(today)
        
        logger.info("[WATCHLIST][BUILD_JOB] env=%s strategy=%s as_of=%s (today=%s)", env, strategy, as_of, today)
        
        # 유니버스 로드
        universe_repo = UniverseRepo(engine)
        universe_snapshot = universe_repo.get_current_universe_snapshot(env, strategy)
        
        if not universe_snapshot or not universe_snapshot.get("members"):
            logger.error("[WATCHLIST][BUILD_JOB][FAIL] universe empty")
            return 1
        
        members = universe_snapshot["members"]
        logger.info("[WATCHLIST][BUILD_JOB] universe loaded members=%s", len(members))
        
        # OHLCV provider 생성
        kis = KisAPI()
        krx_provider = KRXOHLCVProvider()
        kis_provider = KISOHLCVProvider(kis)
        ohlcv_provider = ChainOHLCVProvider([krx_provider, kis_provider])
        
        def _fetch_daily(code: str, count: int = 100):
            """pb1_engine._fetch_daily 호환 래퍼"""
            df = ohlcv_provider.fetch_daily(code, count=count)
            return df, "chain"
        
        # Minervini config
        minervini_config = MinerviniConfig()
        minervini_config_dict = {
            "rs_min": minervini_config.rs_min,
            "breakout_vol_mult_20": minervini_config.breakout_vol_mult_20,
            "heavy_vol_mult_10": minervini_config.heavy_vol_mult_10,
            "add_on_R": minervini_config.add_on_R,
            "max_pyramid_levels": minervini_config.max_pyramid_levels,
            "initial_stop_pct": minervini_config.initial_stop_pct,
            "time_stop_days": minervini_config.time_stop_days,
            "risk_pct_of_equity": minervini_config.risk_pct_of_equity,
            "add_on_size_frac": minervini_config.add_on_size_frac,
            "add_on_max_extension": minervini_config.add_on_max_extension,
        }
        
        # Watchlist 빌드 & 저장
        watchlist = build_and_save_watchlist(
            engine=engine,
            env=env,
            strategy=strategy,
            as_of=as_of,  # ✅ 주간 키
            members=members,
            ohlcv_provider=_fetch_daily,
            minervini_config=minervini_config_dict,
            force_rebuild=True,
        )
        
        logger.info("[WATCHLIST][BUILD_JOB] SUCCESS count=%s as_of=%s", len(watchlist), as_of)
        return 0
        
    except Exception as exc:
        logger.exception("[WATCHLIST][BUILD_JOB][FAIL] err=%s", exc)
        return 1


def _deepcopy_json(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _log_db_only_universe_precheck(
    *,
    repo: UniverseRepo,
    env: str,
    strategy: str,
    namespace: str = "default",
) -> None:
    snapshot = repo.get_current_universe_snapshot(env, strategy)
    if not snapshot:
        logger.warning(
            "[UNIVERSE][LOAD][DB_ONLY][MISS] source=db_only env=%s strategy=%s namespace=%s",
            env,
            strategy,
            namespace,
        )
        return
    logger.info(
        "[UNIVERSE][LOAD][DB_ONLY] source=db_only env=%s strategy=%s namespace=%s run_id=%s as_of=%s members=%s sample=%s",
        env,
        strategy,
        namespace,
        snapshot.get("run_id"),
        snapshot.get("as_of"),
        snapshot.get("members_count"),
        snapshot.get("sample_codes"),
    )


def get_balance_state(
    *,
    kis: KisAPI,
    now: datetime,
) -> tuple[str, dict | None, str | None]:
    try:
        balance_snapshot_raw, balance_source, raw_snapshot = kis.get_balance_cached(
            force=False,
            return_source=True,
            return_raw=True,
        )
        balance_snapshot_raw = _deepcopy_json(balance_snapshot_raw)
        raw_snapshot = _deepcopy_json(raw_snapshot)
        if not isinstance(balance_snapshot_raw, dict):
            raise KisBalanceUnavailable("balance_snapshot_not_dict")
        if not isinstance(balance_snapshot_raw.get("output1"), list):
            raise KisBalanceUnavailable("balance_output1_not_list")
        if not isinstance(balance_snapshot_raw.get("output2"), dict):
            raise KisBalanceUnavailable("balance_output2_not_dict")
        return BALANCE_STATE_OK, balance_snapshot_raw, balance_source
    except KisBalanceUnavailable as exc:
        logger.warning("[PB1][BALANCE][UNAVAILABLE] %s", exc)
    except Exception:
        logger.exception("[PB1][BALANCE][FAIL] initial snapshot")
    return BALANCE_STATE_UNKNOWN, None, None


def _diag_balance_probe_once_safe(*, logger, runtime_root_dir: Path, kis_factory):
    """
    DIAG에서 잔고/예수금/주문가능을 1회만 조회하고, flag 파일로 중복 실행 방지.
    - 전역변수/스코프 의존 금지
    - 런타임 경로를 하드코딩하지 않고 runtime_root 사용
    """
    import os
    import json
    import time

    diag_dir = runtime_root_dir / "runtime" / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    flag_path = diag_dir / "diag_balance_once.flag"
    if flag_path.exists():
        logger.info("[DIAG][BALANCE] already probed -> skip flag=%s", flag_path)
        return

    try:
        with flag_path.open("w", encoding="utf-8") as f:
            f.write(str(int(time.time())))
    except Exception as e:
        logger.warning("[DIAG][BALANCE] failed to write flag: %s", e)

    kis = kis_factory()
    raw = kis.get_balance(force=True)

    out2 = None
    try:
        out2 = (raw or {}).get("output2")
        if isinstance(out2, list) and out2:
            out2 = out2[0]
    except Exception:
        out2 = None

    def _to_int(x):
        try:
            if x is None:
                return None
            s = str(x).strip().replace(",", "")
            if s == "":
                return None
            return int(float(s))
        except Exception:
            return None

    def _pick(d, keys):
        if not isinstance(d, dict):
            return None
        for k in keys:
            if k in d:
                v = _to_int(d.get(k))
                if v is not None:
                    return (k, v)
        return None

    candidates_total = [
        "tot_evlu_amt", "evlu_amt_smtl", "tot_asst_amt", "tot_asset_amt",
        "tot_asst_amt2", "tot_evlu_amt2"
    ]
    candidates_orderable = [
        "ord_psbl_cash", "ord_psbl_amt", "nxdy_excc_amt", "dnca_tot_amt"
    ]
    candidates_deposit = [
        "dnca_tot_amt", "prvs_rcdl_excc_amt", "nxdy_excc_amt", "cma_evlu_amt"
    ]

    picked_total = _pick(out2, candidates_total)
    picked_orderable = _pick(out2, candidates_orderable)
    picked_deposit = _pick(out2, candidates_deposit)

    logger.info("[DIAG][BALANCE][RAW_KEYS] output2_keys=%s",
                sorted(list(out2.keys())) if isinstance(out2, dict) else None)
    logger.info("[DIAG][BALANCE][PICK] total=%s orderable=%s deposit=%s",
                picked_total, picked_orderable, picked_deposit)

    raw_path = os.path.join(diag_dir, "diag_balance_raw.json")
    try:
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False)
        logger.info("[DIAG][BALANCE] saved raw=%s", raw_path)
    except Exception as e:
        logger.warning("[DIAG][BALANCE] failed to save raw json: %s", e)


def ensure_universe_built_once(
    *,
    engine,
    env: str | None = None,
    strategy: str | None = None,
    as_of: str | None = None,
) -> list[dict]:
    env = env or os.getenv("KIS_ENV") or os.getenv("ENV")
    if env is not None:
        env = env.strip()
    strategy = strategy or os.getenv("STRATEGY_NAME", DEFAULT_UNIVERSE_STRATEGY)
    as_of = as_of or _get_now_kst().date().isoformat()

    assert env, "env is required"
    assert strategy, "strategy is required"
    assert as_of, "as_of is required"

    log.info("[UNIVERSE][ENSURE][DB] env=%s strategy=%s as_of=%s", env, strategy, as_of)
    repo = UniverseRepo(engine)
    
    # Step A) 오늘 유니버스 DB 로드
    members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of)
    if members:
        return members

    # Step B) 없으면(빈 리스트) 오늘 유니버스 build & persist 시도
    try:
        from trader.universe import build as universe_build
        built = universe_build.build_universe(as_of_date=as_of, env=env, strategy=strategy)
        if built:
            repo.save_universe_run_and_members(env=env, strategy=strategy, as_of=as_of, members=built)
            members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of)
            if members:
                return members
    except Exception as exc:
        logger.warning("[UNIVERSE][BUILD][FAIL] env=%s strategy=%s as_of=%s err=%s", env, strategy, as_of, exc)

    # Step C) build 실패 또는 DB 오류면 “최근 유니버스 fallback”
    allow_fallback = os.getenv("ALLOW_UNIVERSE_DB_FAIL", "0") == "1"
    if allow_fallback:
        latest = repo.get_latest_universe_members(env=env, strategy=strategy, as_of_date=as_of)
        if latest:
            logger.warning("[UNIVERSE][FALLBACK] using latest as_of<=%s count=%d", as_of, len(latest))
            return latest

    # Step D) fallback도 없으면 “이번 루프 스킵(거래 안 함) + 다음 루프에서 재시도”
    logger.error("[UNIVERSE][EMPTY] no universe available. skip this cycle and retry next loop.")
    return []


def _load_universe_context(
    *,
    engine,
    as_of: str,
    env: str,
    strategy: str,
) -> UniverseContext:
    # [NEW] WATCHLIST_MODE=1이면 WATCHLIST env에서 직접 로딩
    watchlist_mode = os.getenv("WATCHLIST_MODE", "0") == "1"
    if watchlist_mode:
        watchlist_codes = os.getenv("WATCHLIST", "005930,000660,035420,035720,005380")
        codes = [c.strip() for c in watchlist_codes.split(",") if c.strip()]
        members = [{"code": code, "name": code} for code in codes]
        logger.info(
            "[PB1][WATCHLIST_MODE] bypassing universe -> using %d codes: %s",
            len(codes),
            codes[:10],
        )
        return UniverseContext(
            as_of_date=as_of,
            members=members,
            selected_path=None,
            meta={"source": "watchlist_env", "codes": codes},
            is_empty=False,
        )
    
    repo = UniverseRepo(engine)
    members = repo.get_universe_members(env=env, strategy=strategy, as_of_date=as_of)
    if not members and is_db_only_mode():
        fallback = repo.get_current_universe_snapshot(env, strategy)
        if fallback and fallback.get("members"):
            logger.warning(
                "[PB1][UNIVERSE][DB_ONLY] fallback_current run_id=%s as_of=%s members=%s",
                fallback.get("run_id"),
                fallback.get("as_of"),
                fallback.get("members_count"),
            )
            return UniverseContext(
                as_of_date=str(fallback.get("as_of") or as_of),
                members=fallback.get("members") or [],
                selected_path=None,
                meta={"source": "db_only", "as_of": fallback.get("as_of")},
                is_empty=False,
            )
        latest = repo.get_latest_successful_universe_snapshot(env=env, strategy=strategy, as_of_date=as_of)
        if latest and latest.get("members"):
            logger.warning(
                "[PB1][UNIVERSE][DB_ONLY] fallback_latest run_id=%s as_of=%s members=%s",
                latest.get("run_id"),
                latest.get("as_of"),
                latest.get("members_count"),
            )
            return UniverseContext(
                as_of_date=str(latest.get("as_of") or as_of),
                members=latest.get("members") or [],
                selected_path=None,
                meta={"source": "db_only", "as_of": latest.get("as_of")},
                is_empty=False,
            )
        logger.error("[PB1][UNIVERSE][DB_ONLY][MISS] env=%s strategy=%s as_of=%s", env, strategy, as_of)
        raise RuntimeError("db_only_universe_missing")
    if not members:
        logger.warning(
            "[PB1][UNIVERSE][EMPTY_OK] as_of=%s -> skip trading (오늘은 조건 맞는 종목 없음(미너비니 필터 0))",
            as_of,
        )
        return UniverseContext(
            as_of_date=as_of,
            members=[],
            selected_path=None,
            meta={"source": "db", "as_of": as_of},
            is_empty=True,
        )
    return UniverseContext(
        as_of_date=as_of,
        members=members,
        selected_path=None,
        meta={"source": "db", "as_of": as_of},
        is_empty=False,
    )


def _resolve_market_context(
    *,
    now: datetime,
    trading_day: bool,
    market_window: str,
    window_override: str,
    phase_seed: str | None,
) -> tuple[WindowDecision | None, str, str, str, str, list[str]]:
    window = decide_window(now=now, override=window_override)
    window_label = _resolve_window_label(market_window, window)
    resolved_phase, phase_reason, phase_window = resolve_pb1_phase(now, trading_day, phase_seed)
    reasons: list[str] = []
    if not resolved_phase:
        fallback = (os.getenv("PB1_PHASE_DEFAULT") or "entry").strip().lower()
        resolved_phase = fallback if fallback else "entry"
        phase_reason = "default_env"
        reasons.append("phase_default_env")
    reasons.append(f"market_window:{market_window}")
    reasons.append(f"window:{window_label}")
    reasons.append(f"phase:{resolved_phase}")
    return window, window_label, resolved_phase, phase_reason, phase_window, reasons


def _resolve_window_label(market_window: str, window: WindowDecision | None) -> str:
    # ✅ DIAG_FULL_EXEC이면 window 덮어쓰기 방지
    diag_full_exec = env_bool("PB1_DIAG_FULL_EXEC", False)
    strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
    
    normalized = (market_window or "").strip().lower()
    if normalized in {"morning", "day", "close", "preopen", "after", "afternoon", "off"}:
        if window and window.name and window.name != normalized:
            # DIAG_FULL_EXEC이면 경고 없이 normalized 사용
            if diag_full_exec and strategy_mode == "DIAG":
                return normalized
            
            global _WINDOW_MISMATCH_LOGGED
            if not _WINDOW_MISMATCH_LOGGED:
                logger.warning(
                    "[PB1][WINDOW][WARN] market_window=%s mismatch window=%s -> forcing %s",
                    normalized,
                    window.name,
                    normalized,
                )
                _WINDOW_MISMATCH_LOGGED = True
        return normalized
    return window.name if window else "none"


def _parse_hhmm_to_time(hhmm: str) -> dtime:
    hh, mm = hhmm.split(":")
    return dtime(hour=int(hh), minute=int(mm))


def _next_window_start(now: datetime, window_starts: list[dtime]) -> datetime | None:
    sorted_starts = sorted(window_starts)
    for start in sorted_starts:
        if now.time() < start:
            return now.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
    return None


def _market_session(now: datetime) -> tuple[datetime, datetime]:
    open_t = _parse_hhmm_to_time(MARKET_OPEN_HHMM)
    close_t = _parse_hhmm_to_time(MARKET_CLOSE_HHMM)
    return (
        now.replace(hour=open_t.hour, minute=open_t.minute, second=0, microsecond=0),
        now.replace(hour=close_t.hour, minute=close_t.minute, second=0, microsecond=0),
    )


def _parse_now_override(raw: str | None, label: str) -> datetime | None:
    if not raw:
        return None
    engine_runner: PB1Engine | None = None
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        else:
            dt = dt.astimezone(ZoneInfo("Asia/Seoul"))
        return dt
    except Exception:
        logger.warning("[PB1][SMOKE] invalid %s=%s", label, raw)
        return None


def _get_now_kst() -> datetime:
    override = _parse_now_override(os.getenv("NOW_KST_OVERRIDE"), "NOW_KST_OVERRIDE")
    if override:
        return override
    simulated = _parse_now_override(os.getenv("PB1_SIMULATE_NOW_KST"), "PB1_SIMULATE_NOW_KST")
    if simulated:
        return simulated
    return now_kst()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def decide_market_window(now: datetime) -> str:
    forced_override = (os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
    if forced_override in {"preopen", "morning", "day", "close", "after"}:
        return forced_override

    trading_day_env = os.getenv("TRADING_DAY")
    if trading_day_env is not None:
        trading_day = _env_bool("TRADING_DAY", default=True)
        if not trading_day:
            return "after"

    if not is_trading_weekday(now):
        return "after"

    computed = calc_market_window_kst(now)
    forced = (os.getenv("MARKET_WINDOW") or "").strip().lower()
    if forced in {"preopen", "morning", "day", "close", "after"}:
        return forced

    return computed


def detect_window(now_kst_value: datetime, preopen_start: str = "08:45", preopen_end: str = "09:00") -> str:
    return decide_market_window(now_kst_value)


def _decide_action(now: datetime, trading_day: bool, open_dt: datetime, close_dt: datetime, allow_wait: bool, max_wait_s: int, smoke_enabled: bool) -> tuple[str, datetime | None]:
    if smoke_enabled:
        return "smoke", None
    if not trading_day:
        return "smoke", None
    if now < open_dt:
        remaining = (open_dt - now).total_seconds()
        if allow_wait and remaining <= max_wait_s:
            return "wait", open_dt
        return "smoke", open_dt
    if now >= close_dt:
        return "smoke", None
    return "run", None


def _log_balance_cache(force: bool) -> None:
    logger.info("[BALANCE][CACHE] hit=%s", not force)


def _run_smoke(engine, kis_env: str, now: datetime) -> None:
    token_ok = balance_ok = universe_ok = pretrade_ok = False
    kis: KisAPI | None = None
    members: list[dict] = []
    try:
        kis = KisAPI()
        token_ok = True
    except Exception as exc:
        logger.warning("[SMOKE][FAIL] token_init err=%s", exc)

    if kis:
        try:
            _log_balance_cache(force=True)
            snap = kis.get_balance_cached(force=True)
            balance_ok = bool(snap)
        except Exception:
            logger.exception("[SMOKE][FAIL] balance")

    repo = UniverseRepo(engine)
    try:
        members = repo.get_current_universe_members(kis_env, "best_k_meta")
        if not members:
            if os.getenv("ALLOW_UNIVERSE_BUILD_IN_TRADE", "0") == "1" and not is_db_only_mode():
                from trader.universe import build as universe_build

                universe_build.build_universe(as_of_date=now.date().isoformat(), env=kis_env, strategy="best_k_meta")
                members = repo.get_current_universe_members(kis_env, "best_k_meta")
            else:
                logger.info("[UNIVERSE][SKIP] forbidden during trade path")
        universe_ok = bool(members)
    except Exception:
        logger.exception("[SMOKE][FAIL] universe")

    if kis:
        try:
            code = members[0]["code"] if members else "005930"
            quote = kis.get_price_quote(code, diag_mode=True, attempts=1)
            pretrade_ok = bool(quote)
        except Exception:
            logger.exception("[SMOKE][FAIL] pretrade")

    status = token_ok and balance_ok and universe_ok and pretrade_ok
    if status:
        logger.info(
            "[SMOKE][PASS] token=%s balance=%s universe=%s pretrade=%s",
            token_ok,
            balance_ok,
            universe_ok,
            pretrade_ok,
        )
    else:
        logger.warning(
            "[SMOKE][FAIL] token=%s balance=%s universe=%s pretrade=%s",
            token_ok,
            balance_ok,
            universe_ok,
            pretrade_ok,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PB1 close pullback runner")
    parser.add_argument(
        "--window",
        default="auto",
        choices=["auto", "preopen", "morning", "day", "close"],
        help="Execution window override",
    )
    parser.add_argument("--phase", default="auto", choices=["auto", "entry", "exit", "verify"], help="Phase override")
    return parser.parse_args()


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("[PB1][ENV] invalid %s=%s fallback=%s", name, raw, default)
        return default


def _parse_optional_int_env(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    raw = raw.strip()
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("[PB1][ENV] invalid %s=%s fallback=none", name, raw)
        return None


def _resolve_loop_limits() -> tuple[int, int, int, bool]:
    """
    루프 제한 시간 계산.
    
    Returns:
        (run_loop_minutes, max_minutes, max_seconds, run_loop_configured)
    
    정책:
        - PB1_LOOP_MODE=UNTIL_CLOSE: 장 마감까지 루프 (max_minutes=0으로 무제한)
        - 기존 PB1_RUN_LOOP_MINUTES/PB1_MAX_MINUTES 설정 유지
    """
    loop_mode = os.getenv("PB1_LOOP_MODE", "").upper()
    
    # UNTIL_CLOSE 모드: 장 마감까지 무제한
    if loop_mode == "UNTIL_CLOSE":
        logger.info("[LOOP_LIMITS] mode=UNTIL_CLOSE -> max_minutes=0 (unlimited until close)")
        return 0, 0, 0, True
    
    # 기존 로직 (하위 호환)
    run_loop_minutes_env = _parse_optional_int_env("PB1_RUN_LOOP_MINUTES")
    if run_loop_minutes_env is None:
        run_loop_minutes_env = _parse_optional_int_env("RUN_LOOP_MINUTES")
    run_loop_configured = run_loop_minutes_env is not None
    run_loop_minutes = run_loop_minutes_env if run_loop_minutes_env is not None else 15

    max_minutes_env = _parse_optional_int_env("PB1_MAX_MINUTES")
    max_minutes = max_minutes_env if max_minutes_env is not None else run_loop_minutes

    max_seconds_env = _parse_optional_int_env("PB1_MAX_SECONDS")
    max_seconds = max_seconds_env if max_seconds_env is not None else max(0, max_minutes * 60)

    return run_loop_minutes, max_minutes, max_seconds, run_loop_configured


def _change_flag_path() -> Path:
    cache_root = Path(os.getenv("TRADER_CACHE_ROOT") or runtime_root() / "runtime")
    return cache_root / "changed.flag"


def _write_change_flag(changed: bool, reasons: list[str]) -> None:
    flag_path = _change_flag_path()
    flag_path.parent.mkdir(parents=True, exist_ok=True)
    flag_path.write_text("1\n" if changed else "0\n", encoding="utf-8")
    logger.info("[PB1][CHANGE] changed=%s reasons=%s", int(changed), reasons)


def _write_last_db_write(runtime_root_dir: Path, *, run_id: str | None, reason: str, now: datetime) -> Path:
    payload = {
        "run_id": run_id,
        "reason": reason,
        "ts": now.isoformat(),
    }
    target = runtime_root_dir / "runtime" / "status" / "last_db_write.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def _extract_dnca_total(balance_snapshot: dict | None) -> int | None:
    if not balance_snapshot:
        return None
    summary_raw = balance_snapshot.get("output2")
    summary = summary_raw[0] if isinstance(summary_raw, list) and summary_raw else summary_raw if isinstance(summary_raw, dict) else None
    if not isinstance(summary, dict):
        return None
    raw = summary.get("dnca_tot_amt")
    if raw is None:
        return None
    try:
        return int(float(str(raw).replace(",", "")))
    except Exception:
        return None


def _is_balance_empty(balance_snapshot: dict | None) -> bool:
    if not balance_snapshot:
        return False
    rows = balance_snapshot.get("output1") or []
    if not rows:
        return True
    for row in rows:
        try:
            qty = int(float(str(row.get("hldg_qty") or row.get("ord_psbl_qty") or "0").replace(",", "")))
        except Exception:
            qty = 0
        if qty > 0:
            return False
    return True


def should_degrade(remaining_s: float) -> bool:
    return remaining_s < 60


def run_once(
    *,
    args: argparse.Namespace,
    engine,
    ctx: RunContext,
    loop_mode: bool = False,
    window: WindowDecision | None = None,
    runtime_dir: Path | None = None,
    max_seconds: int = 0,
) -> tuple[list[Path], bool, dict[str, int], str, str]:
    workflow_run_id = None
    for env_var in ["GITHUB_RUN_ID", "GITHUB_RUN_NUMBER", "WORKFLOW_RUN_ID"]:
        value = os.getenv(env_var)
        if value:
            workflow_run_id = str(value)
            break
    
    # [NEW] FORCE_RUN, WATCHLIST_MODE 로깅
    force_run = os.getenv("FORCE_RUN", "0") == "1"
    watchlist_mode = os.getenv("WATCHLIST_MODE", "0") == "1"
    dry_run = os.getenv("DRY_RUN", "0") == "1"
    live_trading = os.getenv("LIVE_TRADING_ENABLED", "0") == "1"
    
    if force_run or watchlist_mode:
        logger.info(
            "[PB1][FORCE_RUN] FORCE_RUN=%s WATCHLIST_MODE=%s WATCHLIST=%s DRY_RUN=%s LIVE_TRADING=%s",
            force_run,
            watchlist_mode,
            os.getenv("WATCHLIST", "")[:100],
            dry_run,
            live_trading,
        )
    
    # [1] LIVE 강제 정책: STRATEGY_MODE=LIVE일 때 env 검증
    if os.getenv("STRATEGY_MODE") == "LIVE":
        violations = []
        if os.getenv("LIVE_TRADING_ENABLED") != "1":
            violations.append("LIVE_TRADING_ENABLED != '1'")
        if os.getenv("DISABLE_LIVE_TRADING") == "1":
            violations.append("DISABLE_LIVE_TRADING == '1'")
        if os.getenv("DRY_RUN") == "1":
            violations.append("DRY_RUN == '1'")
        if os.getenv("DB_ONLY") == "1":
            violations.append("DB_ONLY == '1'")
        if os.getenv("NONTRADING_SMOKE") == "1":
            violations.append("NONTRADING_SMOKE == '1'")
        if violations:
            raise RuntimeError(f"LIVE mode violations: {', '.join(violations)}")
    
    now = _get_now_kst()
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    tick_start_ts = time_mod.monotonic()
    deadline_ts = tick_start_ts + max_seconds if max_seconds > 0 else None
    persist_budget_sec = _parse_int_env("PB1_PERSIST_BUDGET_SEC", 30)
    if max_seconds > 0:
        persist_budget_sec = max(5, min(persist_budget_sec, max_seconds))
    else:
        persist_budget_sec = max(5, persist_budget_sec)
    trade_budget_sec = max(0, max_seconds - persist_budget_sec) if max_seconds > 0 else 0
    run_start_ts = time_mod.time()
    as_of = now.date().isoformat()
    runtime_root_dir = runtime_dir or runtime_root()
    smoke_enabled = os.getenv("PB1_SMOKE_RUN") == "1"
    close_cancel_only = env_bool("PB1_CLOSE_CANCEL_ONLY", False)
    if close_cancel_only:
        logger.info("[PB1][MODE] close_cancel_only=Y")
    event_name = os.getenv("GITHUB_EVENT_NAME", "") or ""
    event_name_lower = event_name.lower()
    mode, trading_day, market_window, mode_source = resolve_strategy_mode(
        now_kst=now,
        force_mode_env=os.getenv("FORCE_STRATEGY_MODE"),
    )
    trading_day_detected = trading_day
    if os.getenv("TRADING_DAY") is not None:
        trading_day = _env_bool("TRADING_DAY", default=trading_day)
    auto_window = decide_market_window(now)
    if auto_window != market_window:
        logger.info(
            "[PB1][WINDOW][AUTO] now_kst=%s env_window=%s computed_window=%s -> using computed",
            now.isoformat(),
            market_window,
            auto_window,
        )
        market_window = auto_window
    effective_mode = mode
    if smoke_enabled:
        trading_day = True
    diag_rehearsal_active = _env_bool("PB1_DIAG_REHEARSAL", False) and _env_bool("DIAG_ALLOW_NONTRADING_RUN", False)
    if diag_rehearsal_active and not trading_day:
        logger.warning(
            "[PB1][DIAG] non-trading-day override enabled (trading_day=%s -> forced True)",
            trading_day_detected,
        )
        trading_day = True
    logger.info(
        "[MODE_DECISION] source=%s now_kst=%s trading_day=%s window=%s mode=%s",
        mode_source,
        now.isoformat(),
        trading_day,
        market_window,
        mode,
    )
    os.environ["STRATEGY_MODE"] = mode
    diag_env_flag = (
        env_bool("DIAGNOSTIC_FORCE_RUN", False)
        or env_bool("DIAGNOSTIC_ONLY", DIAGNOSTIC_ONLY)
        or env_bool("DIAGNOSTIC_MODE", DIAGNOSTIC_MODE)
    )
    open_dt, close_dt = _market_session(now)
    allow_wait = env_bool("PB1_ALLOW_WAIT", env_bool("PB1_WAIT_FOR_WINDOW", PB1_WAIT_FOR_WINDOW))
    max_wait_s = int(PB1_MAX_WAIT_FOR_WINDOW_MIN) * 60
    entry_flag = parse_env_flag("PB1_ENTRY_ENABLED", default=PB1_ENTRY_ENABLED)
    force_phase_env = os.getenv("FORCE_PB1_PHASE") or ""
    phase_seed = force_phase_env if force_phase_env else (None if args.phase == "auto" else args.phase)
    resolved_window, window_label, resolved_phase, phase_reason, phase_window, context_reasons = _resolve_market_context(
        now=now,
        trading_day=trading_day,
        market_window=market_window,
        window_override=args.window,
        phase_seed=phase_seed,
    )
    
    # ✅ DIAG_FULL_EXEC: DIAG 모드에서 window/phase 강제 우회
    diag_full_exec = env_bool("PB1_DIAG_FULL_EXEC", False)
    if diag_full_exec and mode == "DIAG":
        logger.info("[PB1][DIAG_FULL_EXEC] force window=day, phase=entry (bypass window/phase gates)")
        market_window = "day"
        window_label = "day"
        resolved_phase = "entry"
        phase_reason = "diag_full_exec_override"
        context_reasons.append("diag_full_exec:forced_day_entry")
    
    if window is not None:
        resolved_window = window
        window_label = _resolve_window_label(market_window, window)
        context_reasons.append("window:locked")
    window = resolved_window
    phase_for_log = resolved_phase

    os.environ.setdefault("MORNING_WINDOW_START", MORNING_WINDOW_START)
    os.environ.setdefault("MORNING_WINDOW_END", MORNING_WINDOW_END)
    os.environ.setdefault("MORNING_EXIT_START", MORNING_EXIT_START)
    os.environ.setdefault("MORNING_EXIT_END", MORNING_EXIT_END)
    os.environ.setdefault("AFTERNOON_WINDOW_START", AFTERNOON_WINDOW_START)
    os.environ.setdefault("AFTERNOON_WINDOW_END", AFTERNOON_WINDOW_END)
    os.environ.setdefault("CLOSE_AUCTION_START", CLOSE_AUCTION_START)
    os.environ.setdefault("CLOSE_AUCTION_END", CLOSE_AUCTION_END)

    action = "run"
    target_start = None
    if not loop_mode:
        action, target_start = _decide_action(now, trading_day, open_dt, close_dt, allow_wait, max_wait_s, smoke_enabled)
        logger.info(
            "[PB1][RUN-PLAN] event=%s now_kst=%s trading_day=%s action=%s target_start=%s max_wait_s=%s window=%s phase=%s allow_wait=%s",
            event_name_lower or "unknown",
            now.isoformat(),
            trading_day,
            action,
            target_start.isoformat() if target_start else "none",
            max_wait_s,
            window_label,
            phase_for_log,
            allow_wait,
        )

        if action == "wait" and target_start:
            while True:
                now = _get_now_kst()
                remaining = (target_start - now).total_seconds()
                if remaining <= 0:
                    break
                if remaining > max_wait_s:
                    logger.info(
                        "[PB1][WAIT] now_kst=%s next_open_kst=%s sleeping_s=0 reason=exceeds_max",
                        now.isoformat(),
                        target_start.isoformat(),
                    )
                    action = "smoke"
                    break
                sleep_for = remaining if remaining < 30 else min(60, remaining)
                logger.info("[PB1][WAIT] now_kst=%s next_open_kst=%s sleeping_s=%.0f", now.isoformat(), target_start.isoformat(), sleep_for)
                time_mod.sleep(sleep_for)
            if action == "wait":
                now = _get_now_kst()
                mode, trading_day, market_window, mode_source = resolve_strategy_mode(
                    now_kst=now,
                    force_mode_env=os.getenv("FORCE_STRATEGY_MODE"),
                )
                effective_mode = mode
                os.environ["STRATEGY_MODE"] = mode
                resolved_window, window_label, resolved_phase, phase_reason, phase_window, context_reasons = _resolve_market_context(
                    now=now,
                    trading_day=trading_day,
                    market_window=market_window,
                    window_override=args.window,
                    phase_seed=phase_seed,
                )
                window = resolved_window
                phase_for_log = resolved_phase
                logger.info(
                    "[MODE_DECISION] source=%s now_kst=%s trading_day=%s window=%s mode=%s",
                    mode_source,
                    now.isoformat(),
                    trading_day,
                    market_window,
                    mode,
                )
                logger.info(
                    "[PB1][PHASE] now=%s window=%s phase=%s reason=%s entry_enabled=%s phase_window=%s",
                    now.isoformat(),
                    window_label,
                    resolved_phase,
                    phase_reason,
                    entry_flag.value,
                    phase_window,
                )
                action = "run" if trading_day else "smoke"
                logger.info("[PB1][WAIT][DONE] now_kst=%s window=%s phase=%s", now.isoformat(), window_label, phase_for_log)
    
    # ✅ 장외 스킵은 LIVE 모드에서만 적용 (DIAG는 계속 실행)
    if not trading_day:
        # DIAG 모드: 장외라도 엔진 실행 (단 KIS API는 차단됨)
        if mode == "DIAG":
            logger.info(
                "[PB1][DIAG][NONTRADING_DAY] mode=DIAG continue execution (KIS blocked)",
            )
            # nontrading_smoke는 실행하되, 엔진도 계속 진행
            exit_status = "DIAG_NONTRADING_CONTINUE"
            as_of = now.date().isoformat()
            smoke_flag = nontrading_smoke_flag_path(runtime_root_dir, as_of=as_of)
            if not smoke_flag.exists() or NONTRADING_SMOKE_FORCE:
                try:
                    run_nontrading_smoke_once(
                        runtime_root_dir=runtime_root_dir,
                        engine=engine,
                        now=now,
                        env=(os.getenv("KIS_ENV") or "practice").lower(),
                        strategy=os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY,
                        as_of=as_of,
                        timeout_sec=NONTRADING_SMOKE_TIMEOUT_SEC,
                        db_store=NONTRADING_SMOKE_DB_STORE,
                        force_rebuild=NONTRADING_SMOKE_FORCE_REBUILD,
                    )
                    write_nontrading_smoke_flag(
                        runtime_root_dir,
                        now=now,
                        run_id=os.getenv("GITHUB_RUN_ID", "local"),
                        sha=os.getenv("GITHUB_SHA", "unknown"),
                        as_of=as_of,
                    )
                except Exception as exc:
                    logger.warning("[NONTRADING_SMOKE][FAIL] err=%s", exc)
            # ✅ DIAG는 계속 실행 (return 하지 않음)
        else:
            # LIVE 모드: 장외면 스킵
            exit_status = "NONTRADING_DAY_EXIT"
            if loop_mode:
                logger.info("[PB1][LOOP] non-trading-day -> skip (LIVE mode)")
            else:
                logger.info("[PB1][SKIP] non-trading-day -> skip (LIVE mode)")
            logger.info(
                "[PB1][EXIT] reason=nontrading_day_exit mode=LIVE",
            )
            return [], False, {}, phase_for_log, exit_status

    # ✅ DIAG + PB1_DIAG_FULL_EXEC=1이면 smoke 건너뛰고 엔진 실행
    diag_full = os.getenv("PB1_DIAG_FULL_EXEC", "0") in ("1", "true", "TRUE", "yes", "YES")
    if action == "smoke" and mode == "DIAG" and diag_full:
        logger.info("[PB1][DIAG_FULL_EXEC] bypass smoke -> run engine once (no KIS HTTP)")
        action = "run"  # smoke 건너뛰고 엔진 실행
    elif action == "smoke":
        _run_smoke(engine, kis_env=(os.getenv("KIS_ENV") or "practice").lower(), now=now)
        return [], False, {}, phase_for_log, "SMOKE"

    # ✅ DIAG_FULL이면 윈도우 게이트 무시하고 계속 진행
    if not window and not close_cancel_only:
        if mode == "DIAG" and diag_full:
            # ✅ window 타입 유지: WindowDecision 객체로 생성
            from trader.window_router import WindowDecision
            phase_default = os.getenv("PB1_PHASE_DEFAULT", "entry")
            window = WindowDecision(name="day", phase=phase_default)
            window_label = window.name
            phase_for_log = phase_default
            logger.info("[PB1][DIAG_FULL_EXEC] override window gate -> proceed (window=%s, phase=%s)", window.name, phase_default)
        else:
            logger.info("[PB1][WINDOW] outside active windows override=%s now=%s", args.window, now)
            return [], False, {}, phase_for_log, "OUTSIDE_WINDOW"

    non_trading_day = not trading_day
    force_diag = diag_env_flag

    dry_run_flag = parse_env_flag("DRY_RUN", default=False)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=False)
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=False)
    expect_live_flag = env_bool("EXPECT_LIVE_TRADING", False)
    mode_resolved = resolve_mode(os.getenv("STRATEGY_MODE", ""))
    dry_run_reasons: list[str] = []
    diag_enabled = force_diag
    if diag_enabled:
        dry_run_reasons.append("diagnostic_mode")
    if mode_resolved == "INTENT_ONLY":
        dry_run_reasons.append("STRATEGY_MODE=INTENT_ONLY")
    if mode_resolved != "LIVE":
        dry_run_reasons.append("STRATEGY_MODE!=LIVE")
    if parse_env_flag("DISABLE_LIVE_TRADING", default=disable_live_flag.value).value:
        dry_run_reasons.append("DISABLE_LIVE_TRADING=1")
    live_trading_flag = parse_env_flag("LIVE_TRADING_ENABLED", default=live_trading_flag.value)
    disable_live_flag = parse_env_flag("DISABLE_LIVE_TRADING", default=disable_live_flag.value)
    dry_run_flag = parse_env_flag("DRY_RUN", default=dry_run_flag.value)
    if not live_trading_flag.value and mode_resolved == "LIVE":
        dry_run_reasons.append("LIVE_TRADING_ENABLED=0")
    if dry_run_flag.value:
        dry_run_reasons.append("DRY_RUN=1")
    for flag in (dry_run_flag, disable_live_flag, live_trading_flag):
        if not flag.valid:
            dry_run_reasons.append(f"{flag.name}=invalid({flag.raw})")

    dry_run = bool(dry_run_reasons)
    dry_run_reason = ",".join(dry_run_reasons) if dry_run_reasons else "live"

    logger.info(
        "[PB1][DRY_RUN_RESOLVE] event=%s dry_run=%s reasons=%s",
        event_name_lower or "unknown",
        dry_run,
        dry_run_reasons or ["live"],
    )
    if smoke_enabled:
        logger.info(
            "[PB1][SMOKE] enabled=True simulated_now_kst=%s force_dry_run=True",
            now.isoformat(),
        )

    expect_kis_env = os.getenv("EXPECT_KIS_ENV")
    kis_env_raw = (os.getenv("KIS_ENV") or "").strip()
    kis_env = kis_env_raw.lower()
    api_base_url = (os.getenv("API_BASE_URL") or "").lower()
    guard_live = expect_live_flag and not diag_enabled and trading_day and not dry_run
    if guard_live:
        guard_failures: list[str] = []
        if dry_run:
            guard_failures.append("dry_run")
        if not live_trading_flag.value or not live_trading_flag.valid:
            guard_failures.append("LIVE_TRADING_ENABLED!=1")
        if disable_live_flag.value or not disable_live_flag.valid:
            guard_failures.append("DISABLE_LIVE_TRADING!=0")
        if mode_resolved != "LIVE":
            guard_failures.append("STRATEGY_MODE!=LIVE")
        if kis_env != "practice":
            guard_failures.append("KIS_ENV!=practice")
        if "openapivts" not in api_base_url:
            guard_failures.append("API_BASE_URL missing openapivts")
        if expect_kis_env and kis_env_raw != expect_kis_env:
            guard_failures.append("EXPECT_KIS_ENV mismatch")
        if guard_failures:
            raise SystemExit(f"EXPECT_LIVE_TRADING=1 guards failed: {guard_failures}")

    def _apply_env_flags(dry: bool) -> None:
        os.environ["DRY_RUN"] = "1" if dry else "0"
        os.environ["DISABLE_LIVE_TRADING"] = "1" if disable_live_flag.value else "0"
        os.environ["LIVE_TRADING_ENABLED"] = "1" if live_trading_flag.value else "0"
        os.environ["STRATEGY_MODE"] = effective_mode

    _apply_env_flags(dry_run)

    force_phase_env = os.getenv("FORCE_PB1_PHASE") or ""
    phase_override_arg = resolved_phase
    
    # ✅ 방어: window가 bool로 잘못 설정되지 않았는지 체크
    if isinstance(window, bool):
        raise RuntimeError(f"BUG: window became bool. check DIAG_FULL override. window={window}")
    
    if (
        window
        and event_name_lower == "push"
        and args.phase == "auto"
        and not force_phase_env
        and window.name == "day"
        and env_bool("PB1_FORCE_ENTRY_ON_PUSH", PB1_FORCE_ENTRY_ON_PUSH)
    ):
        try:
            start = datetime.fromisoformat(f"{now.date()}T{PB1_MORNING_WINDOW_END}")
            end = datetime.fromisoformat(f"{now.date()}T{PB1_ENTRY_WINDOW_END}")
            in_afternoon = start.time() <= now.time() < end.time()
        except Exception:
            in_afternoon = False
        if trading_day and in_afternoon:
            logger.info("[PB1][PHASE_OVERRIDE] event=push from=%s to=entry reason=PB1_FORCE_ENTRY_ON_PUSH", phase_override_arg)
            phase_override_arg = "entry"
            phase_reason = "force_push"

    if diag_enabled:
        dry_run = True
        dry_run_reason = dry_run_reason if dry_run_reason else "diagnostic"
        dry_run_reasons = dry_run_reasons or ["diagnostic"]
        if args.phase == "auto" and not force_phase_env:
            phase_override_arg = "verify"
            phase_reason = "diagnostic"
        window = window or WindowDecision(name="diagnostic", phase=phase_override_arg or "verify")
        _apply_env_flags(dry_run)

    window_label = _resolve_window_label(market_window, window)
    phase_for_log = phase_override_arg or "none"
    context_reasons = [r for r in context_reasons if not r.startswith("window:") and not r.startswith("phase:")]
    context_reasons.append(f"window:{window_label}")
    context_reasons.append(f"phase:{phase_for_log}")
    logger.info(
        "[PB1][PHASE] now=%s window=%s phase=%s reason=%s entry_enabled=%s phase_window=%s",
        now.isoformat(),
        window_label,
        phase_for_log,
        phase_reason,
        entry_flag.value,
        phase_window,
    )

    logger.info(
        "[PB1][TICK] now_kst=%s market_window=%s window=%s phase=%s reasons=%s",
        now.isoformat(),
        market_window,
        window_label,
        phase_for_log,
        context_reasons or ["none"],
    )

    def _remaining_seconds() -> float:
        if deadline_ts is None:
            return float("inf")
        return max(0.0, deadline_ts - time_mod.monotonic())

    remaining_s = _remaining_seconds()
    exit_short_circuit = phase_for_log == "exit" or window_label == "close"
    if exit_short_circuit:
        logger.info("[PB1][EXIT_SHORTCIRCUIT] start remaining_s=%.1f", remaining_s)
        kis = None
        try:
            kis = KisAPI()
        except Exception:
            logger.exception("[PB1][EXIT_SHORTCIRCUIT] KIS init failed")
        run_id = os.getenv("GITHUB_RUN_ID", "local")
        reconcile_ok = False
        close_stale_ok = False
        try:
            if kis:
                try:
                    ctx = RunContext(
                        run_id=run_id,
                        env=(os.getenv("KIS_ENV") or "practice").lower(),
                        strategy="pb1_pullback_close",
                        started_at=now,
                        dry_run=False,
                    )
                    reconcile_today(
                        engine=engine,
                        kis=kis,
                        ctx=ctx,
                    )
                    reconcile_ok = True
                except Exception:
                    logger.exception("[PB1][EXIT_SHORTCIRCUIT] reconcile_today failed")
            try:
                close_stale_positions(
                    engine=engine,
                    env=(os.getenv("KIS_ENV") or "practice").lower(),
                    strategy="pb1_pullback_close",
                    reason="exit_phase",
                    ts=now,
                )
                close_stale_ok = True
            except Exception:
                logger.exception("[PB1][EXIT_SHORTCIRCUIT] close_stale_positions failed")
            _write_last_db_write(runtime_root_dir, run_id=run_id, reason="exit_shortcircuit", now=now)
            if not reconcile_ok or not close_stale_ok:
                logger.error(
                    "[PB1][EXIT_SHORTCIRCUIT][PARTIAL_FAIL] reconcile_ok=%s close_stale_ok=%s - continuing with caution",
                    reconcile_ok,
                    close_stale_ok,
                )
            logger.info("[PB1][EXIT_SHORTCIRCUIT] done")
            return [], True, {}, phase_for_log, "EXIT_SHORTCIRCUIT"
        except Exception:
            logger.exception("[PB1][EXIT_SHORTCIRCUIT] failed")

    if should_degrade(remaining_s):
        logger.warning("[PB1][DEGRADED] remaining_s=%.1f -> reconcile+persistent only", remaining_s)
        kis = None
        try:
            kis = KisAPI()
        except Exception:
            logger.exception("[PB1][DEGRADED] KIS init failed")
        run_id = os.getenv("GITHUB_RUN_ID", "local")
        reconcile_ok = False
        close_stale_ok = False
        try:
            if kis:
                try:
                    ctx = RunContext(
                        run_id=run_id,
                        env=(os.getenv("KIS_ENV") or "practice").lower(),
                        strategy="pb1_pullback_close",
                        started_at=now,
                        dry_run=False,
                    )
                    reconcile_today(
                        engine=engine,
                        kis=kis,
                        ctx=ctx,
                    )
                    reconcile_ok = True
                except Exception:
                    logger.exception("[PB1][DEGRADED] reconcile_today failed")
            try:
                close_stale_positions(
                    engine=engine,
                    env=(os.getenv("KIS_ENV") or "practice").lower(),
                    strategy="pb1_pullback_close",
                    reason="budget_degraded",
                    ts=now,
                )
                close_stale_ok = True
            except Exception:
                logger.exception("[PB1][DEGRADED] close_stale_positions failed")
            _write_last_db_write(runtime_root_dir, run_id=run_id, reason="budget_degraded", now=now)
            if not reconcile_ok or not close_stale_ok:
                logger.error(
                    "[PB1][DEGRADED][PARTIAL_FAIL] reconcile_ok=%s close_stale_ok=%s - continuing with caution",
                    reconcile_ok,
                    close_stale_ok,
                )
            return [], True, {}, phase_for_log, "DEGRADED_BUDGET"
        except Exception:
            logger.exception("[PB1][DEGRADED] failed")

    if not loop_mode:
        if is_db_only_mode():
            universe_strategy = os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
            _log_db_only_universe_precheck(
                repo=UniverseRepo(engine),
                env=kis_env or "practice",
                strategy=universe_strategy,
            )
        universe_members = ensure_universe_built_once(engine=engine, as_of=as_of)
        if not universe_members:
            logger.warning("[PB1] universe empty -> skip trading cycle")
            return touched_files, False, {}, phase_for_log, "SKIP_EMPTY_UNIVERSE"

    logger.info(
        "[PB1][RUN-START] event=%s now_kst=%s trading_day=%s market_window=%s window=%s phase=%s phase_reason=%s DRY_RUN=%s DISABLE_LIVE_TRADING=%s LIVE_TRADING_ENABLED=%s STRATEGY_MODE=%s PB1_ENTRY_ENABLED=%s reasons=%s",
        event_name_lower or "unknown",
        now.isoformat(),
        trading_day,
        market_window,
        window_label,
        phase_for_log,
        phase_reason,
        dry_run,
        os.getenv("DISABLE_LIVE_TRADING"),
        os.getenv("LIVE_TRADING_ENABLED"),
        os.getenv("STRATEGY_MODE"),
        os.getenv("PB1_ENTRY_ENABLED"),
        dry_run_reasons or ["live"],
    )

    if non_trading_day:
        logger.info("[PB1][SKIP] non-trading-day(%s) → diagnostics/dry-run reason=%s", now.date(), dry_run_reason)
        if diag_enabled:
            logger.warning("[PB1][DIAG] non-trading-day(%s) but running diagnostics", now.date())

    runs_repo = RunsRepo(engine)
    # Ensure run row exists early to prevent FK errors
    runs_repo.upsert_run(
        run_id=ctx.run_id,
        env=ctx.env,
        strategy=ctx.strategy,
        workflow_run_id=str(ctx.gh_run_number) if ctx.gh_run_number else None,
        ts_start=ctx.started_at,
    )
    universe_repo = UniverseRepo(engine)
    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    positions_repo = PositionsRepo(engine)
    ledger_repo = LedgerEventsRepo(engine)

    run_record_id = None
    engine_runner: PB1Engine | None = None
    did_work = False
    touched_files: list[Path] = []
    result = None
    db_write_reasons: list[str] = []
    universe_ctx: UniverseContext | None = None
    try:
        if not close_cancel_only and trading_day and market_window in {"preopen", "morning", "day", "close"}:
            universe_strategy = os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY
            try:
                universe_ctx = _load_universe_context(
                    engine=engine,
                    as_of=as_of,
                    env=kis_env or "practice",
                    strategy=universe_strategy,
                )
            except RuntimeError as exc:
                logger.error("[PB1][UNIVERSE][FAIL] as_of=%s err=%s", as_of, exc)
                raise
            if getattr(universe_ctx, "is_empty", False):
                logger.info(
                    "[PB1][SKIP] empty universe -> no candidates today; phase=%s window=%s",
                    phase_for_log,
                    window_label,
                )
                return touched_files, False, {}, phase_for_log, "SKIP_EMPTY_UNIVERSE"
        kis: KisAPI | None = None
        try:
            kis = KisAPI()
            if kis.env != kis_env:
                dry_run_reasons.append("kis_env_mismatch")
                dry_run = True
                _apply_env_flags(dry_run)
        except Exception:
            logger.exception("[PB1] KIS init failed -> skip tick")
            return touched_files, False, {}, phase_for_log, "SKIP_KIS_INIT"

        balance_snapshot_raw: dict | None = None
        balance_source: str | None = None
        balance_state = BALANCE_STATE_UNKNOWN
        if kis:
            _log_balance_cache(force=False)
            balance_state, balance_snapshot_raw, balance_source = get_balance_state(
                kis=kis,
                now=now,
            )
        logger.info(
            "[PB1][BALANCE][STATE] state=%s source=%s",
            balance_state,
            balance_source,
        )
        if balance_state == BALANCE_STATE_STALE_OK:
            logger.warning("[PB1][BALANCE][STALE_OK] using recent snapshot for exits")

        user_entry_enabled = bool(entry_flag.value)
        entry_allowed_this_tick = user_entry_enabled
        entry_block_reason = None
        
        # [2] 거래시간 체크: LIVE 모드에서 장중 여부 판정
        if mode == "LIVE":
            is_weekday = now.weekday() < 5  # Mon-Fri
            market_open = datetime.strptime("09:00", "%H:%M").time()
            market_close = datetime.strptime("15:20", "%H:%M").time()
            in_market_hours = is_weekday and (market_open <= now.time() < market_close)
            if not in_market_hours:
                logger.info("[PB1][LIVE][OUT_OF_MARKET] now=%s weekday=%s -> no entry/exit", now.isoformat(), is_weekday)
                entry_allowed_this_tick = False
                entry_block_reason = entry_block_reason or "out_of_market_hours"
        
        if not user_entry_enabled:
            entry_allowed_this_tick = False
            entry_block_reason = "entry_disabled"
        if balance_state == BALANCE_STATE_UNKNOWN:
            entry_allowed_this_tick = False
            entry_block_reason = entry_block_reason or "balance_unknown"
        elif PB1_REQUIRE_BALANCE_FOR_ENTRY and balance_state == BALANCE_STATE_STALE_OK:
            entry_allowed_this_tick = False
            entry_block_reason = entry_block_reason or "balance_stale"
        if window_label not in {"preopen", "morning", "day"}:
            entry_allowed_this_tick = False
            entry_block_reason = entry_block_reason or "window_blocked"
        entry_cutoff_raw = (os.getenv("ENTRY_CUTOFF_TIME") or PB1_ENTRY_WINDOW_END or "").strip()
        if entry_cutoff_raw:
            try:
                cutoff_time = datetime.strptime(entry_cutoff_raw, "%H:%M").time()
                cutoff_dt = datetime.combine(now.date(), cutoff_time, tzinfo=now.tzinfo)
                logger.info(
                    "[PB1][TIME] now_kst=%s cutoff=%s close=%s phase=%s",
                    now.isoformat(),
                    cutoff_dt.isoformat(),
                    close_dt.isoformat(),
                    resolved_phase,
                )
                if now.time() > cutoff_time:
                    entry_allowed_this_tick = False
                    entry_block_reason = entry_block_reason or "entry_cutoff"
            except ValueError:
                logger.warning("[PB1][ENV] invalid ENTRY_CUTOFF_TIME=%s", entry_cutoff_raw)

        if market_window == "preopen":
            if not PB1_ALLOW_PREOPEN_ENTRY:
                entry_allowed_this_tick = False
                entry_block_reason = entry_block_reason or "preopen_block"
            elif PB1_PREOPEN_REQUIRE_BALANCE and balance_state != BALANCE_STATE_OK:
                entry_allowed_this_tick = False
                entry_block_reason = entry_block_reason or "balance_unknown"

        if deadline_ts:
            remaining_budget = deadline_ts - time_mod.monotonic()
            if remaining_budget <= persist_budget_sec:
                entry_allowed_this_tick = False
                entry_block_reason = entry_block_reason or "timeout_budget"
                logger.warning(
                    "[PB1][TIMEOUT][ENTRY_BLOCKED] remaining=%.1fs persist_budget=%s trade_budget=%s",
                    remaining_budget,
                    persist_budget_sec,
                    trade_budget_sec,
                )

        if kis and getattr(kis, "safe_mode", False):
            entry_allowed_this_tick = False
            entry_block_reason = entry_block_reason or "safe_mode"
            logger.warning("[PB1][SAFE_MODE] entry_allowed=0")
            try:
                ReconcileLogRepo(engine).append_log(
                    env=kis_env or "practice",
                    strategy="pb1_pullback_close",
                    tick_ts=now,
                    action="safe_mode_entry_block",
                    details_json={"reason": "safe_mode"},
                )
            except Exception:
                logger.warning("[PB1][SAFE_MODE][RECONCILE_LOG][FAIL]", exc_info=True)

        if not entry_allowed_this_tick:
            logger.info(
                "[PB1][ENTRY_BLOCKED] reason=%s entry_allowed=0",
                entry_block_reason or "unknown",
            )

        run_record_id = runs_repo.start_run(
            env=kis_env or "practice",
            strategy="pb1_pullback_close",
            run_window=window_label,
            phase=phase_override_arg,
            event_name=event_name_lower,
            dry_run=dry_run,
            git_sha=os.getenv("GITHUB_SHA"),
            workflow=os.getenv("GITHUB_WORKFLOW"),
            workflow_run_id=workflow_run_id,
            workflow_attempt=int(os.getenv("GITHUB_RUN_ATTEMPT", "0") or 0),
            config_json={
                "dry_run_reasons": dry_run_reasons,
                "run_window": window_label,
                "phase": phase_override_arg,
                "phase_reason": phase_reason,
            },
        )
        db_write_reasons.append("run_start")
        reconcile_result: dict | None = None
        if kis:
            try:
                reconcile_result = reconcile_kis(
                    engine=engine,
                    kis=kis,
                    env=kis_env or "practice",
                    run_id=run_record_id,
                    strategy="pb1_pullback_close",
                    tick_ts=now,
                    balance_snapshot=balance_snapshot_raw,
                    runtime_dir=str(runtime_root_dir),
                )
                db_write_reasons.append("reconcile")
                if not reconcile_result.get("ok", True):
                    logger.warning(
                        "[PB1][RECONCILE][DEGRADED] reason=%s err=%s",
                        reconcile_result.get("reason"),
                        reconcile_result.get("err"),
                    )
            except KisTemporaryError as exc:
                logger.warning("[PB1][RECONCILE][DEGRADED] %s", exc)
            except Exception as exc:
                if not dry_run and mode_resolved == "LIVE":
                    logger.error("[PB1][RECONCILE][FAIL] %s", exc)
                logger.warning("[PB1][RECONCILE][WARN] %s", exc)

        if balance_state == BALANCE_STATE_UNKNOWN and PB1_REQUIRE_BALANCE_FOR_ENTRY:
            logger.warning("[PB1][DEGRADED] reason=balance_unknown -> skip trading")
            runs_repo.finish_run(run_record_id, status="DEGRADED", notes="balance_unknown")
            db_write_reasons.append("balance_degraded")
            _write_last_db_write(runtime_root_dir, run_id=str(run_record_id), reason="balance_degraded", now=now)
            return [], False, {}, phase_for_log, "DEGRADED_BALANCE_UNKNOWN"

        engine_runner = PB1Engine(
            universe_repo=universe_repo,
            orders_repo=orders_repo,
            fills_repo=fills_repo,
            positions_repo=positions_repo,
            ledger_repo=ledger_repo,
            kis=kis,
            window=window,
            window_label=window_label,
            phase=phase_override_arg,
            dry_run=dry_run,
            env=kis_env or "practice",
            run_id=run_record_id,
            strategy=universe_strategy,  # [FIX] watchlist 버그 수정 - strategy 전달
            now_kst_value=now,
            balance_snapshot=balance_snapshot_raw,
            balance_source=balance_source,
            entry_allowed_this_tick=entry_allowed_this_tick,
            entry_block_reason=entry_block_reason,
            preopen_max_new_positions=PB1_PREOPEN_MAX_NEW_POSITIONS if market_window == "preopen" else 0,
            universe_context=universe_ctx,
            diag_full_exec=diag_full_exec,  # ✅ DIAG 풀패스 플래그 전달
        )
        
        # ✅ DIAG_FULL_EXEC 실행 로그
        if diag_full_exec and mode == "DIAG":
            logger.info(
                "[PB1][DIAG_FULL_EXEC] calling engine.run() window=%s phase=%s dry_run=%s",
                window_label,
                phase_override_arg,
                dry_run,
            )
        
        if close_cancel_only:
            result = engine_runner.run_close_cancel()
        else:
            result = engine_runner.run()
        # Save top_candidates for next run to limit OHLCV queries
        if engine_runner and hasattr(engine_runner, 'top_candidates'):
            top_candidates_path = runtime_path("top_candidates.json")
            try:
                with open(top_candidates_path, 'w') as f:
                    json.dump(engine_runner.top_candidates, f)
                logger.info("[PB1][TOP_CANDIDATES][SAVE] saved %s to %s", len(engine_runner.top_candidates), top_candidates_path)
            except Exception as exc:
                logger.warning("[PB1][TOP_CANDIDATES][SAVE_FAIL] %s", exc)
        did_work = True
        runs_repo.finish_run(run_record_id, status=result.status, notes=result.notes)
        db_write_reasons.append("run_finish")
        _write_last_db_write(
            runtime_root_dir,
            run_id=str(run_record_id),
            reason=",".join(db_write_reasons),
            now=now,
        )
        touched_files = []
    except Exception as exc:
        logger.exception("[PB1][FAIL] unexpected error")
        phase_context = phase_for_log or phase_override_arg or "unknown"
        window_context = window_label or "unknown"
        current_code = engine_runner.current_code if engine_runner else None
        top_candidates = engine_runner.top_candidates if engine_runner else []
        logger.error(
            "[PB1][FAIL][CONTEXT] phase=%s window=%s current_code=%s top_candidates=%s",
            phase_context,
            window_context,
            current_code,
            top_candidates,
        )
        if run_record_id:
            runs_repo.finish_run(run_record_id, status="FAILED", notes=str(exc))
            _write_last_db_write(runtime_root_dir, run_id=str(run_record_id), reason="failed", now=now)
        raise
    finally:
        pass
    metrics = {
        "balance_api_calls": result.balance_api_calls if result else 0,
        "balance_cache_hits": result.balance_cache_hits if result else 0,
        "balance_tick_cache_hits": result.balance_tick_cache_hits if result else 0,
    }
    result_status = result.status if result else "UNKNOWN"
    
    # ✅ DIAG 모드 실행 요약 로그
    if mode == "DIAG":
        logger.info(
            "[DIAG_SUMMARY] status=%s phase=%s did_work=%s metrics=%s",
            result_status,
            phase_for_log,
            did_work,
            metrics,
        )
        if engine_runner:
            logger.info(
                "[DIAG_SUMMARY][ENGINE] top_candidates=%s current_code=%s",
                len(engine_runner.top_candidates) if hasattr(engine_runner, 'top_candidates') else 0,
                engine_runner.current_code if hasattr(engine_runner, 'current_code') else None,
            )
    
    return touched_files, did_work, metrics, phase_for_log, result_status


def _run_nontrading_smoke_if_needed(
    *,
    runtime_root_dir: Path,
    engine,
    now: datetime,
    strategy_mode: str,
) -> bool:
    if strategy_mode != "DIAG":
        return False
    _, trading_day, _market_window, _mode_source = resolve_strategy_mode(
        now_kst=now,
        force_mode_env=os.getenv("FORCE_STRATEGY_MODE"),
    )
    if trading_day:
        return False
    as_of = now.date().isoformat()
    smoke_flag = nontrading_smoke_flag_path(runtime_root_dir, as_of=as_of)
    if smoke_flag.exists() and not NONTRADING_SMOKE_FORCE:
        logger.info("[NONTRADING_SMOKE][SKIP] reason=already_done flag=%s", smoke_flag)
        return False
    run_nontrading_smoke_once(
        runtime_root_dir=runtime_root_dir,
        engine=engine,
        now=now,
        env=(os.getenv("KIS_ENV") or "practice").lower(),
        strategy=os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY,
        as_of=as_of,
        timeout_sec=NONTRADING_SMOKE_TIMEOUT_SEC,
        db_store=NONTRADING_SMOKE_DB_STORE,
        force_rebuild=NONTRADING_SMOKE_FORCE_REBUILD,
    )
    write_nontrading_smoke_flag(
        runtime_root_dir,
        now=now,
        run_id=os.getenv("GITHUB_RUN_ID", "local"),
        sha=os.getenv("GITHUB_SHA", "unknown"),
        as_of=as_of,
    )
    return True


def _run_loop(*, args: argparse.Namespace) -> None:
    loop_interval = _parse_int_env("PB1_LOOP_INTERVAL_SEC", 60)
    run_loop_minutes, loop_max_minutes, max_seconds, _ = _resolve_loop_limits()
    loop_mode = os.getenv("PB1_LOOP_MODE", "").upper()
    now = _get_now_kst()
    
    # ✅ UNTIL_CLOSE 모드: 장 마감까지 루프
    if loop_mode == "UNTIL_CLOSE":
        loop_deadline_dt = compute_loop_deadline(now)
        logger.info(
            "[PB1][LOOP] mode=UNTIL_CLOSE interval=%s deadline=%s",
            loop_interval,
            loop_deadline_dt.strftime("%Y-%m-%d %H:%M:%S"),
        )
    else:
        # 기존 로직 (max_minutes 기반)
        _, close_dt = _market_session(now)
        logger.info(
            "[PB1][LOOP] enabled interval=%s close=%s max_minutes=%s run_loop_minutes=%s",
            loop_interval,
            close_dt.isoformat(),
            loop_max_minutes,
            run_loop_minutes,
        )
    
    logger.info(
        "[PB1][LOOP] start now_kst=%s max_seconds=%s",
        now.isoformat(),
        max_seconds,
    )

    total_start_ts = time_mod.monotonic()
    engine = make_engine()
    lock_conn = engine.connect()
    if not acquire_advisory_lock(lock_conn):
        logger.warning("[PB1][LOOP] run lock unavailable -> exit")
        lock_conn.close()
        return
    exit_reason = "unknown"
    balance_api_calls = 0
    balance_cache_hits = 0
    balance_tick_cache_hits = 0
    last_phase = "none"
    loop_started_ts = total_start_ts
    loop_deadline = None
    loop_deadline_ts = None
    runtime_root_dir = runtime_root()
    
    # Create RunContext for loop mode
    env = os.getenv("ENV", "live")
    strategy = os.getenv("STRATEGY", "best_k_meta")
    gh_run_number = _parse_optional_int_env("GITHUB_RUN_ID") or _parse_optional_int_env("GITHUB_RUN_NUMBER")
    git_sha = os.getenv("GITHUB_SHA")
    ctx = RunContext.new(env=env, strategy=strategy, gh_run_number=gh_run_number, git_sha=git_sha)
    logger.info(
        "[PB1][LOOP][CONTEXT] run_id=%s env=%s strategy=%s gh_run_number=%s git_sha=%s",
        ctx.run_id,
        ctx.env,
        ctx.strategy,
        ctx.gh_run_number,
        ctx.git_sha,
    )
    
    try:
        run_migrations(engine)
        _write_change_flag(False, ["init"])
        trade_start_ts = time_mod.monotonic()
        loop_started_ts = trade_start_ts
        loop_deadline_ts = trade_start_ts + max_seconds if max_seconds > 0 else None
        loop_deadline = None
        now_kst_value = _get_now_kst()
        if max_seconds > 0:
            loop_deadline = now_kst_value + timedelta(seconds=max_seconds)
        logger.info(
            "[PB1][CLOCK] total_start=%.3f trade_start=%.3f",
            total_start_ts,
            trade_start_ts,
        )
        logger.info(
            "[PB1][LOOP] deadline_ready now_kst=%s deadline=%s max_seconds=%s baseline=trade",
            now_kst_value.isoformat(),
            loop_deadline.isoformat() if loop_deadline else "none",
            max_seconds,
        )
        ensure_universe_built_once(
            engine=engine,
            as_of=now_kst_value.date().isoformat(),
        )
        strategy_mode = (
            getattr(args, "strategy_mode", None)
            or os.getenv("EFFECTIVE_STRATEGY_MODE")
            or os.getenv("STRATEGY_MODE")
            or ""
        )
        strategy_mode = str(strategy_mode).upper()
        kis_factory = lambda: KisAPI()
        if strategy_mode == "DIAG":
            logger.info("[DIAG][BALANCE] probe_once start")
            _diag_balance_probe_once_safe(
                logger=logger,
                runtime_root_dir=runtime_root_dir,
                kis_factory=kis_factory,
            )
        stop_requested = {"value": False}

        def _request_stop(reason: str) -> None:
            if stop_requested["value"]:
                return
            stop_requested["value"] = True
            logger.warning("[PB1][SIGNAL] %s -> stopping loop", reason)

        signal.signal(signal.SIGTERM, lambda *_args: _request_stop("SIGTERM"))
        signal.signal(signal.SIGINT, lambda *_args: _request_stop("SIGINT"))
        while True:
            if stop_requested["value"]:
                exit_reason = "sigterm"
                break
            now = _get_now_kst()
            
            # ✅ UNTIL_CLOSE 모드: deadline 체크
            if loop_mode == "UNTIL_CLOSE":
                if now >= loop_deadline_dt:
                    logger.info("[PB1][LOOP] deadline reached -> exit now=%s deadline=%s",
                                now.strftime("%H:%M:%S"), loop_deadline_dt.strftime("%H:%M:%S"))
                    exit_reason = "loop_deadline"
                    break
            
            if _run_nontrading_smoke_if_needed(
                runtime_root_dir=runtime_root_dir,
                engine=engine,
                now=now,
                strategy_mode=strategy_mode,
            ):
                exit_reason = "nontrading_smoke_done"
                break
            
            # 기존 close_dt 체크 (UNTIL_CLOSE가 아닐 때만)
            if loop_mode != "UNTIL_CLOSE":
                _, close_dt = _market_session(now)
                if now >= close_dt:
                    logger.info("[PB1][LOOP] market closed -> exit")
                    exit_reason = "market_closed"
                    break
            
            elapsed_seconds = time_mod.monotonic() - loop_started_ts
            if max_seconds > 0 and elapsed_seconds >= max_seconds:
                logger.info("[PB1][LOOP] max_seconds=%s exiting", max_seconds)
                exit_reason = "loop_timeout"
                break
            if loop_max_minutes > 0:
                elapsed_min = (time_mod.monotonic() - loop_started_ts) / 60
                if elapsed_min >= loop_max_minutes:
                    logger.info("[PB1][LOOP] max minutes reached -> exit elapsed_min=%.1f", elapsed_min)
                    exit_reason = "loop_timeout"
                    break
            window = decide_window(now=now, override=args.window)
            if window is None:
                if max_seconds > 0 and os.getenv("PB1_LOOP_WAIT_OUTSIDE", "0") != "1":
                    next_start = _next_window_start(
                        now,
                        [
                            _parse_hhmm_to_time(MORNING_WINDOW_START),
                            _parse_hhmm_to_time(PB1_MORNING_WINDOW_END),
                            _parse_hhmm_to_time(PB1_ENTRY_WINDOW_END),
                        ],
                    )
                    logger.info(
                        "[PB1][LOOP] outside window -> exit (tick mode) next=%s",
                        next_start.isoformat() if next_start else "unknown",
                    )
                    exit_reason = "outside_window"
                    break

                next_start = _next_window_start(
                    now,
                    [
                        _parse_hhmm_to_time(MORNING_WINDOW_START),
                        _parse_hhmm_to_time(PB1_MORNING_WINDOW_END),
                        _parse_hhmm_to_time(PB1_ENTRY_WINDOW_END),
                    ],
                )
                if next_start:
                    sleep_for = (next_start - now).total_seconds()
                else:
                    sleep_for = loop_interval
                sleep_for = max(5.0, min(300.0, sleep_for))

                # FIX: RUN_LOOP_MINUTES(=max_seconds) 예산보다 더 오래 sleep 해야 하면
                # sleep 후 깨어나자마자 loop_timeout으로 끝나 "한 번 돌고 죽는" 것처럼 보인다.
                # 이 경우에는 sleep하지 않고 종료하여 다음 5분 tick(schedule)에 맡긴다.
                if max_seconds > 0:
                    remaining_budget = max_seconds - elapsed_seconds
                    if remaining_budget <= 0:
                        logger.info("[PB1][LOOP] budget exhausted before sleep -> exit")
                        exit_reason = "loop_timeout"
                        break
                    if sleep_for >= remaining_budget:
                        logger.info(
                            "[PB1][LOOP] outside window but insufficient budget -> exit "
                            "sleep=%.0fs remaining_budget=%.0fs next=%s",
                            sleep_for,
                            remaining_budget,
                            next_start.isoformat() if next_start else "unknown",
                        )
                        exit_reason = "outside_window_budget"
                        break
                logger.info(
                    "[PB1][LOOP] outside window -> sleep %.0fs next=%s",
                    sleep_for,
                    next_start.isoformat() if next_start else "unknown",
                )
                time_mod.sleep(sleep_for)
                continue

            if loop_deadline_ts and time_mod.monotonic() > loop_deadline_ts:
                logger.warning("[PB1][LOOP] deadline_exceeded_pre_trade deadline=%s", loop_deadline.isoformat())
                exit_reason = "deadline_exceeded_pre_trade"
                break
            remaining_budget_s = 0
            if loop_deadline_ts:
                remaining_budget_s = max(0, int(loop_deadline_ts - time_mod.monotonic()))
            try:
                _touched, _did_work, metrics, last_phase, result_status = run_once(
                    args=args,
                    engine=engine,
                    ctx=ctx,
                    loop_mode=True,
                    window=window,
                    max_seconds=remaining_budget_s,
                )
                balance_api_calls += metrics.get("balance_api_calls", 0)
                balance_cache_hits += metrics.get("balance_cache_hits", 0)
                balance_tick_cache_hits += metrics.get("balance_tick_cache_hits", 0)
                if result_status in {"NONTRADING_SMOKE_DONE", "NONTRADING_DAY_EXIT"}:
                    logger.info("[PB1][LOOP] non-trading-day exit status=%s", result_status)
                    exit_reason = result_status.lower()
                    break
                if result_status == "NO_TRADE":
                    logger.info("[PB1][LOOP] no trade -> exit")
                    exit_reason = "no_candidates"
                    break
            except Exception as exc:
                logger.error(
                    "[PB1][TICK][FATAL_GUARD] exception=%s\n%s",
                    exc,
                    traceback.format_exc(),
                )
                time_mod.sleep(3)
            time_mod.sleep(loop_interval)
        elapsed_trade = time_mod.monotonic() - loop_started_ts
        elapsed_total = time_mod.monotonic() - total_start_ts
        if exit_reason == "unknown":
            exit_reason = "shutdown"
        if max_seconds > 0 and elapsed_trade > max_seconds:
            logger.warning(
                "[PB1][EXIT][WARN] reason=%s elapsed_trade=%.1fs elapsed_total=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
                exit_reason,
                elapsed_trade,
                elapsed_total,
                max_seconds,
                loop_deadline.isoformat() if loop_deadline else "none",
                last_phase,
                balance_api_calls,
                balance_cache_hits,
                balance_tick_cache_hits,
            )
        else:
            logger.info(
                "[PB1][EXIT] reason=%s elapsed_trade=%.1fs elapsed_total=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
                exit_reason,
                elapsed_trade,
                elapsed_total,
                max_seconds,
                loop_deadline.isoformat() if loop_deadline else "none",
                last_phase,
                balance_api_calls,
                balance_cache_hits,
                balance_tick_cache_hits,
            )
    finally:
        try:
            release_advisory_lock(lock_conn)
        except Exception as exc:
            logger.warning("[PB1][LOCK][RELEASE_FAIL] advisory lock release failed (ignoring): %s", exc)
        try:
            lock_conn.close()
        except Exception as exc:
            logger.warning("[PB1][LOCK][CLOSE_FAIL] lock connection close failed (ignoring): %s", exc)


def _exit_code_for_status(status: str) -> int:
    if status in {"FAILED", "ERROR"}:
        return 1
    return 0


def main() -> int:
    # ✅ 설계 1: JOB 모드 분리 (BUILD_WATCHLIST vs TRADE_INTRADAY)
    job_mode = os.getenv("PB1_JOB", "TRADE_INTRADAY").upper()
    
    if job_mode == "BUILD_WATCHLIST":
        logger.info("[PB1][JOB] mode=BUILD_WATCHLIST -> build weekly watchlist and exit")
        return _run_build_watchlist_job()
    
    # else: TRADE_INTRADAY (기존 로직)
    logger.info("[PB1][JOB] mode=TRADE_INTRADAY -> run entry/exit logic")
    
    # ✅ AUTO 모드 결정 및 환경변수 고정
    mode_env = os.getenv("STRATEGY_MODE", "AUTO")
    resolved_mode = resolve_auto_strategy_mode(mode_env)
    os.environ["STRATEGY_MODE"] = resolved_mode
    logger.info(
        "[PB1][MODE] mode_env=%s resolved=%s fixed_in_env=True",
        mode_env,
        resolved_mode,
    )
    
    args = parse_args()
    assert_db_ready()
    smoke_enabled = os.getenv("PB1_SMOKE_RUN") == "1"
    run_loop_minutes, _max_minutes, max_seconds, loop_configured = _resolve_loop_limits()
    
    # ✅ PB1_LOOP_ENABLED 또는 loop_configured로 루프 활성화
    loop_enabled = os.getenv("PB1_LOOP_ENABLED", "0") == "1"
    run_loop = loop_enabled or os.getenv("PB1_RUN_LOOP", "0") == "1" or loop_configured
    
    # ✅ DIAG 모드에서는 루프 비활성화 (한 번만 실행)
    if resolved_mode == "DIAG":
        logger.info("[PB1][DIAG] mode=DIAG -> disable loop (run once)")
        run_loop = False
    
    if run_loop and not smoke_enabled:
        try:
            _run_loop(args=args)
        except Exception:
            logger.error("[PB1][FATAL_GUARD] loop crashed", exc_info=True)
        return 0
    engine = make_engine()
    lock_conn = engine.connect()
    if not acquire_advisory_lock(lock_conn):
        logger.warning("[PB1][RUN] run lock unavailable -> exit")
        lock_conn.close()
        return 0
    run_migrations(engine)
    _write_change_flag(False, ["init"])
    # Create RunContext
    env = os.getenv("ENV", "live")
    strategy = os.getenv("STRATEGY", "best_k_meta")
    gh_run_number = _parse_optional_int_env("GITHUB_RUN_ID") or _parse_optional_int_env("GITHUB_RUN_NUMBER")
    git_sha = os.getenv("GITHUB_SHA")
    ctx = RunContext.new(env=env, strategy=strategy, gh_run_number=gh_run_number, git_sha=git_sha)
    logger.info("[RUN_CONTEXT] run_id=%s env=%s strategy=%s gh_run_number=%s git_sha=%s", ctx.run_id, ctx.env, ctx.strategy, ctx.gh_run_number, ctx.git_sha)
    metrics: dict[str, int] = {}
    phase_for_log = "none"
    result_status = "UNKNOWN"
    start_ts = time_mod.time()
    try:
        if smoke_enabled:
            run_once(args=args, engine=engine, ctx=ctx, loop_mode=False, window=None)
            return 0
        _touched, _did_work, metrics, phase_for_log, result_status = run_once(
            args=args,
            engine=engine,
            ctx=ctx,
            loop_mode=False,
            window=None,
            max_seconds=max_seconds,
        )
    except Exception:
        logger.error("[PB1][FATAL_GUARD] unexpected error", exc_info=True)
        result_status = "ERROR"
    finally:
        try:
            release_advisory_lock(lock_conn)
        except Exception as exc:
            logger.warning("[PB1][LOCK][RELEASE_FAIL] advisory lock release failed (ignoring): %s", exc)
        try:
            lock_conn.close()
        except Exception as exc:
            logger.warning("[PB1][LOCK][CLOSE_FAIL] lock connection close failed (ignoring): %s", exc)
        elapsed = time_mod.time() - start_ts
        logger.info(
            "[PB1][EXIT] reason=single_run elapsed=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
            elapsed,
            max_seconds,
            "none",
            phase_for_log,
            metrics.get("balance_api_calls", 0),
            metrics.get("balance_cache_hits", 0),
            metrics.get("balance_tick_cache_hits", 0),
        )
    if result_status == "ERROR":
        return 0
    return _exit_code_for_status(result_status)


if __name__ == "__main__":
    raise SystemExit(main())
