from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import signal
import time as time_mod
import copy
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from trader.config import (
    AFTERNOON_WINDOW_END,
    AFTERNOON_WINDOW_START,
    BOT_STATE_RESET,
    BOT_STATE_HARD_RESET,
    BOT_STATE_RESET_CASH_MAX_KRW,
    BOT_STATE_RESET_ON_ACCOUNT_FP_MISMATCH,
    BOT_STATE_RESET_ON_EMPTY_KIS_HOLDINGS,
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
from trader.botstate_paths import botstate_path, ensure_not_repo_tracked_path, get_botstate_root
from trader.botstate_sync import (
    BotStateContext,
    acquire_lock as acquire_botstate_lock,
    compute_lock_ttl,
    ensure_sqlite_writable,
    hard_reset_bot_state,
    persist_run_files,
    release_lock as release_botstate_lock,
    resolve_botstate_worktree_dir,
    setup_worktree,
)
from trader.db.engine import make_engine
from trader.db.lock import release_lock, try_acquire_lock
from trader.db.migrate import run_migrations
from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo, RunsRepo, UniverseRepo
from trader.kis_wrapper import KisAPI, KisBalanceUnavailable, KisTemporaryError
from trader.ledger.store import LedgerStore
from trader.pb1_engine import PB1Engine, resolve_pb1_phase
from trader.reconcile_kis import reconcile_today
from trader.reconcile_db import close_stale_positions
from trader.reset_utils import detect_account_fp, purge_bot_state
from trader.runtime_store import DEFAULT_UNIVERSE_STRATEGY, RuntimeStore
from trader.time_utils import now_kst
from trader.eventlog import emit_event
from trader.utils.env import env_bool, parse_env_flag, resolve_mode
from trader.utils.json_sanitize import to_jsonable
from trader.window_router import WindowDecision, decide_window

logger = logging.getLogger(__name__)
log = logger

_WINDOW_MISMATCH_LOGGED = False
_CURRENT_BOTSTATE: dict | None = None

BALANCE_STATE_OK = "OK"
BALANCE_STATE_STALE_OK = "STALE_OK"
BALANCE_STATE_UNKNOWN = "UNKNOWN"


def _deepcopy_json(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return value


def _balance_snapshot_ttl_sec() -> int:
    return _parse_int_env("PB1_BALANCE_SNAPSHOT_TTL_SEC", 120)


def _load_stale_balance_snapshot(runtime_store: RuntimeStore, now: datetime) -> tuple[dict | None, str | None]:
    payload = runtime_store.load_balance_snapshot()
    if not payload:
        return None, None
    ts_raw = payload.get("timestamp_kst")
    if not ts_raw:
        return None, None
    try:
        ts = datetime.fromisoformat(ts_raw)
    except ValueError:
        return None, None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    age_sec = (now - ts).total_seconds()
    if age_sec > _balance_snapshot_ttl_sec():
        return None, None
    snapshot = payload.get("normalized") or payload.get("raw")
    if not isinstance(snapshot, dict):
        return None, None
    return snapshot, "snapshot"


def get_balance_state(
    *,
    kis: KisAPI,
    runtime_store: RuntimeStore,
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
        runtime_store.save_balance_snapshot(
            raw_snapshot=raw_snapshot or balance_snapshot_raw,
            normalized_snapshot=balance_snapshot_raw,
            source="api" if balance_source == "api" else "cache",
            timestamp_kst=now.isoformat(),
        )
        return BALANCE_STATE_OK, balance_snapshot_raw, balance_source
    except KisBalanceUnavailable as exc:
        logger.warning("[PB1][BALANCE][UNAVAILABLE] %s", exc)
    except Exception:
        logger.exception("[PB1][BALANCE][FAIL] initial snapshot")

    stale_snapshot, stale_source = _load_stale_balance_snapshot(runtime_store, now)
    if stale_snapshot:
        return BALANCE_STATE_STALE_OK, stale_snapshot, stale_source
    return BALANCE_STATE_UNKNOWN, None, None


def _diag_balance_probe_once_safe(*, logger, runtime_store, kis_factory):
    """
    DIAG에서 잔고/예수금/주문가능을 1회만 조회하고, flag 파일로 중복 실행 방지.
    - 전역변수/스코프 의존 금지
    - BOT_STATE_DIR 같은 파이썬 변수 사용 금지 (환경변수도 optional fallback)
    """
    import os
    import json
    import time

    runtime_dir = None
    try:
        runtime_dir = getattr(runtime_store, "runtime_dir", None)
        if runtime_dir is None:
            runtime_dir = runtime_store.get_runtime_dir()
    except Exception:
        runtime_dir = None

    if not runtime_dir:
        bot_state_dir = os.getenv("BOT_STATE_DIR") or os.getenv("STATE_DIR") or ""
        if bot_state_dir:
            runtime_dir = os.path.join(bot_state_dir, "runtime")

    if not runtime_dir:
        logger.warning("[DIAG][BALANCE] runtime_dir not resolved -> skip")
        return

    diag_dir = os.path.join(runtime_dir, "diagnostics")
    os.makedirs(diag_dir, exist_ok=True)

    flag_path = os.path.join(diag_dir, "diag_balance_once.flag")
    if os.path.exists(flag_path):
        logger.info("[DIAG][BALANCE] already probed -> skip flag=%s", flag_path)
        return

    try:
        with open(flag_path, "w", encoding="utf-8") as f:
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


def universe_build_flag(as_of: str) -> Path:
    return botstate_path("runtime", f"universe_build_done_{as_of}.flag")


def ensure_universe_built_once(
    runtime_store: RuntimeStore | None = None,
    *,
    env: str | None = None,
    strategy: str | None = None,
    as_of: str | None = None,
    force: bool = False,
    allow_missing: bool = False,
) -> None:
    if runtime_store is None:
        raise ValueError("runtime_store is required for ensure_universe_built_once")

    env = env or os.getenv("KIS_ENV") or os.getenv("ENV")
    if env is not None:
        env = env.strip()
    strategy = strategy or os.getenv("STRATEGY_NAME", DEFAULT_UNIVERSE_STRATEGY)
    as_of = as_of or _get_now_kst().date().isoformat()

    assert env, "env is required"
    assert strategy, "strategy is required"
    assert as_of, "as_of is required"

    log.info(
        "[UNIVERSE][ENSURE] runtime_store=%s env=%s strategy=%s as_of=%s force=%s",
        type(runtime_store).__name__,
        env,
        strategy,
        as_of,
        force,
    )

    ok, meta = runtime_store.universe_check(as_of)
    flag = universe_build_flag(as_of)

    if not force:
        if not ok and not allow_missing:
            return

        if meta.get("have_today"):
            return

        if flag.exists():
            return

    reason = "force" if force else "auto_missing_today"
    flag.parent.mkdir(parents=True, exist_ok=True)
    flag.write_text(f"attempted reason={reason}\n", encoding="utf-8")

    cmd = [
        "python",
        "-m",
        "trader.universe.build",
        "--env",
        env,
        "--strategy",
        strategy,
        "--date",
        as_of,
    ]
    log.warning("[UNIVERSE][BUILD_TRIGGER] as_of=%s reason=%s cmd=%s", as_of, reason, cmd)
    bot_state_dir = Path(runtime_store.bot_state_dir).resolve()
    env_vars = os.environ.copy()
    env_vars["BOT_STATE_DIR"] = str(bot_state_dir)
    env_vars.setdefault("BOTSTATE_ROOT", str(bot_state_dir))
    subprocess.run(cmd, check=False, cwd=str(bot_state_dir.parent), env=env_vars)
    _, post_meta = runtime_store.load_today_universe(as_of)
    if post_meta.get("have_today"):
        log.info(
            "[UNIVERSE][POST_BUILD_CHECK] have_today=1 path=%s",
            post_meta.get("today_path"),
        )
    else:
        log.warning(
            "[UNIVERSE][POST_BUILD_CHECK][FAIL] today universe missing after build -> likely not persisted path=%s",
            post_meta.get("today_path"),
        )


def _write_account_reset_event(base_dir: Path, payload: dict) -> Path:
    events_dir = base_dir / "runtime" / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    path = events_dir / f"account_reset_{now_kst().date().isoformat()}.jsonl"
    ensure_not_repo_tracked_path(path)
    payload = to_jsonable(payload)
    line = json.dumps(payload, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
    logger.info("[PB1][RESET][EVENT] path=%s payload=%s", path, payload)
    return path


def _handle_missing_positions_reset(
    *,
    balance_snapshot: dict | None,
    env: str,
    strategy: str,
    run_id: str | None,
    engine,
    kis: KisAPI | None,
    orders_count: int,
    fills_count: int,
    positions_repo: PositionsRepo,
    ledger_repo: LedgerEventsRepo,
    bot_state_dir: Path,
    reset_on_missing_positions: bool,
    force_safe_exit: bool,
    hard_reset_requested: bool,
) -> bool:
    if not balance_snapshot:
        return False
    holdings_rows = balance_snapshot.get("output1") or []
    kis_codes = {str(row.get("pdno") or row.get("code") or "").zfill(6) for row in holdings_rows}
    kis_codes = {code for code in kis_codes if code}
    if kis_codes:
        return False
    positions = positions_repo.list_positions(env, strategy)
    position_codes = {str(p.get("code") or "").zfill(6) for p in positions if int(p.get("qty") or 0) > 0}
    if not position_codes:
        return False
    if orders_count > 0 or fills_count > 0:
        logger.info(
            "[PB1][RESET][SKIP] holdings_empty=1 recent_orders=%s recent_fills=%s -> no stale reset",
            orders_count,
            fills_count,
        )
        return False
    ledger_store = LedgerStore(LEDGER_BASE_DIR, env=env, run_id=run_id)
    ledger_positions = ledger_store.rebuild_positions_average_cost(lookback_days=LEDGER_LOOKBACK_DAYS)
    ledger_codes = {code for (code, sid, mode) in ledger_positions.keys() if code}
    missing_codes = sorted(position_codes | ledger_codes)
    if not missing_codes:
        return False
    if not reset_on_missing_positions and not force_safe_exit:
        if hard_reset_requested and os.getenv("BOT_STATE_HARD_RESET_DONE") != "1":
            reset_result = hard_reset_bot_state(bot_state_dir, reason="stale_db")
            os.environ["BOT_STATE_HARD_RESET_DONE"] = "1"
            logger.warning(
                "[PB1][RESET][HARD_STALE_DB] env=%s missing_positions=%s result=%s",
                env,
                missing_codes,
                reset_result,
            )
            return True
        closed_count = close_stale_positions(
            engine=engine,
            env=env,
            strategy=strategy,
            reason="stale_db_holdings_empty",
            ts=now_kst(),
        )
        for code in missing_codes:
            ledger_repo.append_event(
                env=env,
                run_id=run_id,
                event_type="AUTO_CLOSED_STALE_KIS_EMPTY",
                ts=now_kst(),
                code=code,
                sid=1,
                mode=1,
                ok=True,
                reasons=["AUTO_CLOSED_STALE_KIS_EMPTY"],
                payload_json={
                    "source": "stale_db_autoclose",
                    "orders": orders_count,
                    "fills": fills_count,
                },
            )
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            event_type="STALE_DB_SOFT_RESET",
            ts=now_kst(),
            ok=True,
            reasons=["holdings_empty", "soft_close"],
            payload_json={
                "closed_positions": closed_count,
                "missing_codes": missing_codes,
                "orders": orders_count,
                "fills": fills_count,
            },
        )
        emit_event(
            as_of=now_kst().date().isoformat(),
            event="stale_db_autoclosed",
            env=env,
            strategy=strategy,
            kis_holdings_count=len(holdings_rows),
            missing_codes=missing_codes,
            ledger_position_count=len(ledger_codes),
            db_position_count=len(position_codes),
            closed_positions=closed_count,
            orders=orders_count,
            fills=fills_count,
            reason="AUTO_CLOSED_STALE_KIS_EMPTY",
        )
        logger.warning(
            "[PB1][RESET][STALE_DB] env=%s holdings_empty=1 missing_positions=%s closed_db_positions=%s",
            env,
            missing_codes,
            closed_count,
        )
        if kis is not None:
            try:
                reconcile_today(engine=engine, kis=kis, env=env, run_id=run_id, strategy=strategy)
            except Exception:
                logger.exception("[PB1][RESET][STALE_DB] reconcile_today failed")
        return False
    closed_count = 0
    if position_codes:
        closed_count = positions_repo.close_positions(env=env, strategy=strategy, codes=sorted(position_codes))
    for code in missing_codes:
        ledger_repo.append_event(
            env=env,
            run_id=run_id,
            event_type="ACCOUNT_RESET_POSITION_CLOSED",
            ts=now_kst(),
            code=code,
            sid=1,
            mode=1,
            ok=True,
            reasons=["account_reset_or_position_missing"],
            payload_json={"source": "missing_position_reset"},
        )
    _write_account_reset_event(
        bot_state_dir,
        {
            "ts": now_kst().isoformat(),
            "env": env,
            "strategy": strategy,
            "reason": "account_reset_or_position_missing",
            "kis_holdings_count": len(holdings_rows),
            "missing_codes": missing_codes,
            "ledger_position_count": len(ledger_codes),
            "db_position_count": len(position_codes),
            "closed_positions": closed_count,
        },
    )
    logger.warning(
        "[PB1][RESET][SAFE_EXIT] env=%s holdings_empty=1 missing_positions=%s closed_db_positions=%s",
        env,
        missing_codes,
        closed_count,
    )
    return True


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
    normalized = (market_window or "").strip().lower()
    if normalized in {"morning", "day", "close", "preopen", "after", "afternoon", "off"}:
        if window and window.name and window.name != normalized:
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


def detect_window(now_kst_value: datetime, preopen_start: str = "08:45", preopen_end: str = "09:00") -> str:
    t = now_kst_value.time()
    ps = dtime.fromisoformat(preopen_start)
    pe = dtime.fromisoformat(preopen_end)
    morning_start = dtime.fromisoformat("09:00")
    morning_end = dtime.fromisoformat(PB1_MORNING_WINDOW_END)
    day_end = dtime.fromisoformat(PB1_ENTRY_WINDOW_END)
    close_end = dtime.fromisoformat(PB1_EXIT_WINDOW_END)
    if ps <= t < pe:
        return "preopen"
    if morning_start <= t < morning_end:
        return "morning"
    if morning_end <= t < day_end:
        return "day"
    if day_end <= t <= close_end:
        return "close"
    return "off"


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
        members = repo.get_latest_universe_members(kis_env, "best_k_meta")
        if not members:
            if os.getenv("ALLOW_UNIVERSE_BUILD_IN_TRADE", "0") == "1":
                from trader.universe import build as universe_build

                universe_build.build_universe(as_of_date=now.date().isoformat(), env=kis_env, strategy="best_k_meta")
                members = repo.get_latest_universe_members(kis_env, "best_k_meta")
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


def _collect_botstate_files(since_ts: float) -> list[Path]:
    base_dir = get_botstate_root()
    if not base_dir.exists():
        touched: list[Path] = []
    else:
        touched = []
    for path in base_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            if path.stat().st_mtime >= since_ts:
                touched.append(path)
        except FileNotFoundError:
            continue
    for env_key in ("STATE_PATH", "LOT_STATE_PATH"):
        env_path = os.getenv(env_key)
        if not env_path:
            continue
        path = Path(env_path)
        if not path.exists():
            continue
        try:
            if path.stat().st_mtime >= since_ts or path not in touched:
                touched.append(path)
        except FileNotFoundError:
            continue
    return touched


def _change_flag_path() -> Path:
    cache_root = Path(os.getenv("TRADER_CACHE_ROOT", "bot_state/runtime"))
    return cache_root / "changed.flag"


def _write_change_flag(changed: bool, reasons: list[str]) -> None:
    flag_path = _change_flag_path()
    flag_path.parent.mkdir(parents=True, exist_ok=True)
    flag_path.write_text("1\n" if changed else "0\n", encoding="utf-8")
    logger.info("[PB1][CHANGE] changed=%s reasons=%s", int(changed), reasons)


def _runtime_meta_path(bot_state_dir: Path) -> Path:
    return bot_state_dir / "runtime" / "runtime_meta.json"


def _load_runtime_meta(bot_state_dir: Path) -> dict:
    path = _runtime_meta_path(bot_state_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("[STATE][META][LOAD_FAIL] path=%s", path)
        return {}


def _write_runtime_meta(bot_state_dir: Path, payload: dict) -> None:
    path = _runtime_meta_path(bot_state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def _is_urgent_persist(touched: list[Path]) -> bool:
    keywords = {"orders", "fills", "exits_intent"}
    for path in touched:
        parts = {part.lower() for part in path.parts}
        if parts & keywords:
            return True
    return False


def _setup_botstate_session(owner: str, run_id: str, ttl_sec: int) -> BotStateContext | None:
    worktree_dir = resolve_botstate_worktree_dir(Path.cwd())
    try:
        ctx = setup_worktree(Path.cwd(), worktree_dir)
    except Exception:
        logger.exception("[BOTSTATE][SETUP] failed worktree=%s", worktree_dir)
        return None
    if not acquire_botstate_lock(worktree_dir, owner=owner, run_id=run_id, ttl_sec=ttl_sec):
        return None
    if BOT_STATE_HARD_RESET and os.getenv("BOT_STATE_HARD_RESET_DONE") != "1":
        reset_result = hard_reset_bot_state(ctx.bot_state_dir, reason="session_start")
        os.environ["BOT_STATE_HARD_RESET_DONE"] = "1"
        logger.info("[BOTSTATE][HARD_RESET][DONE] result=%s", reset_result)
    return ctx


def _register_botstate_ctx(ctx: BotStateContext, owner: str, run_id: str) -> None:
    global _CURRENT_BOTSTATE
    _CURRENT_BOTSTATE = {
        "worktree_dir": ctx.worktree_dir,
        "owner": owner,
        "run_id": run_id,
    }


def _release_botstate_lock_best_effort(reason: str) -> None:
    global _CURRENT_BOTSTATE
    if not _CURRENT_BOTSTATE:
        return
    try:
        release_botstate_lock(
            _CURRENT_BOTSTATE["worktree_dir"],
            _CURRENT_BOTSTATE["owner"],
            _CURRENT_BOTSTATE["run_id"],
        )
        logger.warning("[BOTSTATE][LOCK][RELEASE] reason=%s ok=1", reason)
    except Exception as exc:
        logger.warning("[BOTSTATE][LOCK][RELEASE_FAIL] reason=%s err=%s", reason, exc)


def run_once(
    *,
    args: argparse.Namespace,
    engine,
    loop_mode: bool = False,
    window: WindowDecision | None = None,
    bot_state_dir: Path | None = None,
) -> tuple[list[Path], bool, dict[str, int], str, str]:
    now = _get_now_kst()
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    as_of = now.date().isoformat()
    resolved_bot_state_dir = bot_state_dir or get_botstate_root()
    runtime_store = RuntimeStore(base_dir=resolved_bot_state_dir)
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
    auto_window = detect_window(now, preopen_start=PB1_PREOPEN_START, preopen_end=PB1_PREOPEN_END)
    if auto_window != market_window:
        logger.info(
            "[PB1][WINDOW][AUTO] now_kst=%s env_window=%s auto_window=%s -> using auto",
            now.isoformat(),
            market_window,
            auto_window,
        )
        market_window = auto_window
    effective_mode = mode
    if smoke_enabled:
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
    if not loop_mode:
        allow_missing = market_window == "preopen"
        ensure_universe_built_once(runtime_store=runtime_store, as_of=as_of, allow_missing=allow_missing)

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
    elif not trading_day:
        logger.info("[PB1][LOOP] non-trading-day -> skip")
        return [], False, {}, phase_for_log, "SKIPPED"

    if action == "smoke":
        _run_smoke(engine, kis_env=(os.getenv("KIS_ENV") or "practice").lower(), now=now)
        return [], False, {}, phase_for_log, "SMOKE"

    if not window and not close_cancel_only:
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

    owner = os.getenv("GITHUB_ACTOR", "local")
    workflow_run_id = os.getenv("GITHUB_RUN_ID", "local")
    lock_key = f"PB1:{kis_env_raw or 'practice'}"
    lock_acquired = try_acquire_lock(engine, lock_key)
    if not lock_acquired:
        logger.warning("[PB1][LOCKED] key=%s owner=%s run_id=%s", lock_key, owner, workflow_run_id)
        return [], False, {}, phase_for_log, "LOCKED"

    runs_repo = RunsRepo(engine)
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
    run_start_ts = time_mod.time()
    try:
        if not close_cancel_only and trading_day and market_window in {"preopen", "morning", "day", "close"}:
            universe_ok, universe_meta = runtime_store.universe_check(as_of)
            if not universe_ok:
                if phase_for_log == "entry":
                    logger.info("[PB1][ENTRY_BLOCKED] reason=universe_missing_both entry_allowed=0")
                logger.info("[PB1][SKIP] reason=universe_missing_both as_of=%s", as_of)
                logger.info("[PB1][EXIT_FORCE_RUN] reason=universe_missing_both as_of=%s", as_of)
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
                runtime_store=runtime_store,
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

        if not entry_allowed_this_tick:
            logger.info(
                "[PB1][ENTRY_BLOCKED] reason=%s entry_allowed=0",
                entry_block_reason or "unknown",
            )

        reset_reason = None
        account_fp_now = None
        dnca_total = None
        if kis and balance_snapshot_raw:
            account_fp_now = detect_account_fp(kis.env, kis.CANO, kis.ACNT_PRDT_CD, api_base_url)
            runtime_meta = _load_runtime_meta(resolved_bot_state_dir)
            runtime_fp = runtime_meta.get("account_fp")
            if BOT_STATE_HARD_RESET:
                reset_reason = None
            elif BOT_STATE_RESET:
                reset_reason = "env_reset"
            elif BOT_STATE_RESET_ON_ACCOUNT_FP_MISMATCH and runtime_fp and runtime_fp != account_fp_now:
                reset_reason = "account_fp_mismatch"
            elif BOT_STATE_RESET_ON_EMPTY_KIS_HOLDINGS:
                kis_holdings = balance_snapshot_raw.get("output1") or []
                existing_positions = positions_repo.list_positions(kis_env or "practice", "pb1_pullback_close")
                existing_positions_count = len([p for p in existing_positions if int(p.get("qty") or 0) > 0])
                ledger_store = LedgerStore(LEDGER_BASE_DIR, env=kis_env or "practice", run_id=workflow_run_id)
                ledger_positions = ledger_store.rebuild_positions_average_cost(lookback_days=LEDGER_LOOKBACK_DAYS)
                ledger_has_positions = any(
                    int(state.get("total_qty") or 0) > 0 for state in ledger_positions.values()
                )
                dnca_total = _extract_dnca_total(balance_snapshot_raw)
                is_paper_env = (kis_env or "").lower() != "real"
                if len(kis_holdings) == 0 and (existing_positions_count > 0 or ledger_has_positions) and dnca_total is not None:
                    if is_paper_env and dnca_total == PAPER_MAX_CAPITAL_KRW:
                        reset_reason = "paper_reset_detected"
                        logger.warning(
                            "[PB1][RESET][DETECTED] reason=paper_reset dnca_tot_amt=%s",
                            dnca_total,
                        )
                    elif dnca_total <= BOT_STATE_RESET_CASH_MAX_KRW:
                        reset_reason = "empty_kis_holdings_detected"

        if reset_reason:
            if reset_reason == "paper_reset_detected":
                if (kis_env or "").lower() == "practice" and PAPER_RESET_EVENT_ONLY_IN_PRACTICE:
                    _write_account_reset_event(
                        resolved_bot_state_dir,
                        {
                            "ts": now.isoformat(),
                            "env": kis_env or "practice",
                            "strategy": "pb1_pullback_close",
                            "reason": reset_reason,
                            "action": "event_only",
                            "dnca_total": dnca_total,
                        },
                    )
                    logger.warning("[PB1][RESET] paper_reset_detected in practice -> event_only")
                    reset_reason = None
                elif not PAPER_RESET_AUTO_PURGE:
                    _write_account_reset_event(
                        resolved_bot_state_dir,
                        {
                            "ts": now.isoformat(),
                            "env": kis_env or "practice",
                            "strategy": "pb1_pullback_close",
                            "reason": reset_reason,
                            "action": "event_only",
                            "dnca_total": dnca_total,
                        },
                    )
                    logger.warning("[PB1][RESET] paper_reset_detected -> auto_purge disabled event_only")
                    reset_reason = None
        if reset_reason:
            archive_dir = resolved_bot_state_dir / "archive" / f"reset_{now.strftime('%Y%m%d_%H%M%S')}"
            purge_bot_state(resolved_bot_state_dir, archive_dir, reason=reset_reason)
            run_migrations(engine)
            meta_payload = {"account_fp": account_fp_now} if account_fp_now else {}
            _write_runtime_meta(resolved_bot_state_dir, meta_payload)
        elif account_fp_now:
            runtime_meta = _load_runtime_meta(resolved_bot_state_dir)
            if runtime_meta.get("account_fp") != account_fp_now:
                _write_runtime_meta(resolved_bot_state_dir, {"account_fp": account_fp_now})

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
        reset_on_missing_positions = env_bool("PB1_RESET_ON_MISSING_POSITIONS", False)
        force_safe_exit = env_bool("PB1_FORCE_SAFE_EXIT", False)
        todays_orders = orders_repo.list_today_orders(kis_env or "practice")
        todays_fills = fills_repo.list_today_fills(kis_env or "practice")
        if balance_state == BALANCE_STATE_OK:
            if _handle_missing_positions_reset(
                balance_snapshot=balance_snapshot_raw,
                env=kis_env or "practice",
                strategy="pb1_pullback_close",
                run_id=run_record_id,
                engine=engine,
                kis=kis,
                orders_count=len(todays_orders),
                fills_count=len(todays_fills),
                positions_repo=positions_repo,
                ledger_repo=ledger_repo,
                bot_state_dir=resolved_bot_state_dir,
                reset_on_missing_positions=reset_on_missing_positions,
                force_safe_exit=force_safe_exit,
                hard_reset_requested=BOT_STATE_HARD_RESET,
            ):
                if env_bool("PRACTICE_RESET_DAY", False):
                    logger.warning("[PB1][RESET][ENTRY_BLOCKED] PRACTICE_RESET_DAY=1 -> entry_disabled")
                runs_repo.finish_run(run_record_id, status="RESET_ABORT", notes="account_reset_or_position_missing")
                touched_files = _collect_botstate_files(run_start_ts)
                return touched_files, True, {}, phase_for_log, "RESET_ABORT"
        else:
            logger.warning("[PB1][BALANCE][DEGRADED] state=%s -> skip reset/reconcile", balance_state)
        if balance_state == BALANCE_STATE_UNKNOWN and PB1_REQUIRE_BALANCE_FOR_ENTRY:
            logger.warning("[PB1][DEGRADED] reason=balance_unknown -> skip trading")
            runs_repo.finish_run(run_record_id, status="DEGRADED", notes="balance_unknown")
            touched_files = _collect_botstate_files(run_start_ts)
            return touched_files, False, {}, phase_for_log, "DEGRADED_BALANCE_UNKNOWN"
        if kis and balance_state == BALANCE_STATE_OK:
            try:
                reconcile_result = reconcile_today(
                    engine=engine,
                    kis=kis,
                    env=kis_env or "practice",
                    run_id=run_record_id,
                    strategy="pb1_pullback_close",
                )
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
            now_kst_value=now,
            balance_snapshot=balance_snapshot_raw,
            balance_source=balance_source,
            entry_allowed_this_tick=entry_allowed_this_tick,
            entry_block_reason=entry_block_reason,
            preopen_max_new_positions=PB1_PREOPEN_MAX_NEW_POSITIONS if market_window == "preopen" else 0,
        )
        if close_cancel_only:
            result = engine_runner.run_close_cancel()
        else:
            result = engine_runner.run()
        did_work = True
        runs_repo.finish_run(run_record_id, status=result.status, notes=result.notes)
        touched_files = _collect_botstate_files(run_start_ts)
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
        raise
    finally:
        release_lock(engine, lock_key)
    metrics = {
        "balance_api_calls": result.balance_api_calls if result else 0,
        "balance_cache_hits": result.balance_cache_hits if result else 0,
        "balance_tick_cache_hits": result.balance_tick_cache_hits if result else 0,
    }
    result_status = result.status if result else "UNKNOWN"
    return touched_files, did_work, metrics, phase_for_log, result_status


def _run_loop(*, args: argparse.Namespace) -> None:
    loop_interval = _parse_int_env("PB1_LOOP_INTERVAL_SEC", 60)
    persist_interval = _parse_int_env("PB1_PERSIST_INTERVAL_SEC", 300)
    loop_max_minutes = _parse_int_env("PB1_LOOP_MAX_MINUTES", 0)
    run_loop_minutes = _parse_int_env("RUN_LOOP_MINUTES", 0)
    now = _get_now_kst()
    _, close_dt = _market_session(now)
    max_seconds = run_loop_minutes * 60 if run_loop_minutes > 0 else 0
    if run_loop_minutes > 0:
        loop_max_minutes = run_loop_minutes
        persist_interval = max(120, min(persist_interval, 240))
    logger.info(
        "[PB1][LOOP] enabled interval=%s persist_interval=%s close=%s max_minutes=%s run_loop_minutes=%s",
        loop_interval,
        persist_interval,
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
    owner = os.getenv("GITHUB_ACTOR", "local")
    workflow_run_id = os.getenv("GITHUB_RUN_ID", "local")
    ttl_sec, ttl_buffer = compute_lock_ttl(max_seconds)
    logger.info(
        "[BOTSTATE][LOCK] ttl_sec=%s max_seconds=%s buffer=%s",
        ttl_sec,
        max_seconds,
        ttl_buffer,
    )
    botstate_ctx = _setup_botstate_session(owner=owner, run_id=workflow_run_id, ttl_sec=ttl_sec)
    if botstate_ctx is None:
        logger.warning("[PB1][LOOP] botstate lock unavailable -> exit")
        return
    _register_botstate_ctx(botstate_ctx, owner, workflow_run_id)
    engine = make_engine()
    run_migrations(engine)
    _write_change_flag(False, ["init"])
    runtime_store = RuntimeStore(base_dir=botstate_ctx.bot_state_dir)

    pending_touched: dict[Path, Path] = {}
    last_persist_ts = 0.0
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
    loop_window = detect_window(now_kst_value, preopen_start=PB1_PREOPEN_START, preopen_end=PB1_PREOPEN_END)
    ensure_universe_built_once(
        runtime_store=runtime_store,
        as_of=now_kst_value.date().isoformat(),
        allow_missing=loop_window == "preopen",
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
            runtime_store=runtime_store,
            kis_factory=kis_factory,
        )
    exit_reason = "unknown"
    stop_requested = {"value": False}
    balance_api_calls = 0
    balance_cache_hits = 0
    balance_tick_cache_hits = 0
    last_phase = "none"

    def _request_stop(reason: str) -> None:
        if stop_requested["value"]:
            return
        stop_requested["value"] = True
        logger.warning("[PB1][SIGNAL] %s -> stopping loop", reason)

    signal.signal(signal.SIGTERM, lambda *_args: (_request_stop("SIGTERM"), _release_botstate_lock_best_effort("SIGTERM")))
    signal.signal(signal.SIGINT, lambda *_args: (_request_stop("SIGINT"), _release_botstate_lock_best_effort("SIGINT")))
    try:
        while True:
            if stop_requested["value"]:
                exit_reason = "sigterm"
                break
            now = _get_now_kst()
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
            touched, _did_work, metrics, last_phase, result_status = run_once(
                args=args,
                engine=engine,
                loop_mode=True,
                window=window,
                bot_state_dir=botstate_ctx.bot_state_dir,
            )
            balance_api_calls += metrics.get("balance_api_calls", 0)
            balance_cache_hits += metrics.get("balance_cache_hits", 0)
            balance_tick_cache_hits += metrics.get("balance_tick_cache_hits", 0)
            if result_status == "NO_TRADE":
                logger.info("[PB1][LOOP] no trade -> exit")
                exit_reason = "no_candidates"
                break
            if touched:
                _write_change_flag(True, ["touched_files"])
            for path in touched:
                pending_touched[path] = path
            if pending_touched:
                now_ts = time_mod.monotonic()
                urgent = _is_urgent_persist(list(pending_touched.values()))
                if urgent or (now_ts - last_persist_ts >= persist_interval):
                    persist_run_files(
                        botstate_ctx.worktree_dir,
                        list(pending_touched.values()),
                        message=f"pb1 loop {now.isoformat()}",
                    )
                    pending_touched.clear()
                    last_persist_ts = now_ts
            time_mod.sleep(loop_interval)
    finally:
        elapsed_trade = time_mod.monotonic() - loop_started_ts
        elapsed_total = time_mod.monotonic() - total_start_ts
        if exit_reason == "unknown":
            exit_reason = "shutdown"
        if pending_touched:
            persist_run_files(
                botstate_ctx.worktree_dir,
                list(pending_touched.values()),
                message=f"pb1 loop {now_kst().isoformat()}",
            )
        release_botstate_lock(botstate_ctx.worktree_dir, owner, workflow_run_id)
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


def main() -> None:
    args = parse_args()
    smoke_enabled = os.getenv("PB1_SMOKE_RUN") == "1"
    run_loop = os.getenv("PB1_RUN_LOOP", "0") == "1"
    run_loop_minutes = _parse_int_env("RUN_LOOP_MINUTES", 0)
    if run_loop_minutes > 0:
        run_loop = True
    if run_loop and not smoke_enabled:
        _run_loop(args=args)
        return
    if smoke_enabled:
        bot_state_dir = get_botstate_root()
        ensure_sqlite_writable(bot_state_dir / "db" / "pbcore.sqlite3")
        engine = make_engine()
        run_migrations(engine)
        _write_change_flag(False, ["init"])
        run_once(args=args, engine=engine, loop_mode=False, window=None)
        return

    owner = os.getenv("GITHUB_ACTOR", "local")
    workflow_run_id = os.getenv("GITHUB_RUN_ID", "local")
    ttl_sec, ttl_buffer = compute_lock_ttl(run_loop_minutes * 60)
    logger.info(
        "[BOTSTATE][LOCK] ttl_sec=%s max_seconds=%s buffer=%s",
        ttl_sec,
        run_loop_minutes * 60,
        ttl_buffer,
    )
    botstate_ctx = _setup_botstate_session(owner=owner, run_id=workflow_run_id, ttl_sec=ttl_sec)
    if botstate_ctx is None:
        logger.warning("[PB1][RUN] botstate lock unavailable -> exit")
        return
    _register_botstate_ctx(botstate_ctx, owner, workflow_run_id)
    engine = make_engine()
    run_migrations(engine)
    _write_change_flag(False, ["init"])
    signal.signal(
        signal.SIGTERM,
        lambda *_args: _release_botstate_lock_best_effort("SIGTERM"),
    )
    signal.signal(
        signal.SIGINT,
        lambda *_args: _release_botstate_lock_best_effort("SIGINT"),
    )
    metrics: dict[str, int] = {}
    phase_for_log = "none"
    start_ts = time_mod.time()
    try:
        touched, _did_work, metrics, phase_for_log, _result_status = run_once(
            args=args,
            engine=engine,
            loop_mode=False,
            window=None,
            bot_state_dir=botstate_ctx.bot_state_dir,
        )
        if touched:
            _write_change_flag(True, ["touched_files"])
            persist_run_files(
                botstate_ctx.worktree_dir,
                touched,
                message=f"pb1 run {now_kst().isoformat()}",
            )
        else:
            _write_change_flag(False, ["no_changes"])
    finally:
        release_botstate_lock(botstate_ctx.worktree_dir, owner, workflow_run_id)
        elapsed = time_mod.time() - start_ts
        logger.info(
            "[PB1][EXIT] reason=single_run elapsed=%.1fs max_seconds=%s deadline=%s phase=%s balance_api_calls=%s balance_cache_hits=%s balance_tick_cache_hits=%s",
            elapsed,
            run_loop_minutes * 60,
            "none",
            phase_for_log,
            metrics.get("balance_api_calls", 0),
            metrics.get("balance_cache_hits", 0),
            metrics.get("balance_tick_cache_hits", 0),
        )


if __name__ == "__main__":
    main()
