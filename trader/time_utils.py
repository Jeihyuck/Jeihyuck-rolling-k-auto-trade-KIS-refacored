"""거래일/거래 가능 시간 헬퍼."""

from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
import io
import json
import logging
import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(15, 30)
_PYKRX_WARNED_SIGNATURES: set[tuple[str, str, str]] = set()
_PYKRX_PREV_OR_SAME_CACHE: dict[str, date | None] = {}
_NOISY_EXTERNAL_LOGGERS = (
    "pykrx",
    "pykrx.website",
    "pykrx.website.krx",
    "requests",
    "urllib3",
)

# ---------------------------------------------------------------------------
# KRX Holiday Calendar (fallback when pykrx is unavailable)
# ---------------------------------------------------------------------------

_KRX_HOLIDAYS_CACHE: set[date] | None = None
_KRX_CALENDAR_FALLBACK_LOGGED = False


def _get_krx_holidays_config_path() -> Path:
    """config/krx_holidays.json 경로 반환 (repo root 기준)."""
    # time_utils.py → trader/ → repo root
    candidates = [
        Path(__file__).parent.parent / "config" / "krx_holidays.json",
        Path(os.getcwd()) / "config" / "krx_holidays.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        "config/krx_holidays.json not found. "
        "Check that the file exists in the repository root config/ directory."
    )


def _load_krx_holidays() -> set[date]:
    """config/krx_holidays.json 에서 KRX 휴장일 Set을 로드한다."""
    global _KRX_HOLIDAYS_CACHE
    global _KRX_CALENDAR_FALLBACK_LOGGED
    if _KRX_HOLIDAYS_CACHE is not None:
        return _KRX_HOLIDAYS_CACHE

    try:
        config_path = _get_krx_holidays_config_path()
        raw = json.loads(config_path.read_text(encoding="utf-8"))
        holidays: set[date] = set()
        for year_str, date_list in raw.items():
            if not isinstance(date_list, list):
                continue
            for ds in date_list:
                try:
                    holidays.add(date.fromisoformat(str(ds)))
                except (ValueError, TypeError):
                    logger.warning("[TIME][KRX][CALENDAR_LOAD] invalid date entry=%s", ds)
        _KRX_HOLIDAYS_CACHE = holidays
        logger.debug(
            "[TIME][KRX][CALENDAR_LOAD] loaded %d holidays from %s",
            len(holidays),
            config_path,
        )
        return holidays
    except FileNotFoundError:
        if not _KRX_CALENDAR_FALLBACK_LOGGED:
            logger.info(
                "[TIME][KRX][CALENDAR_FALLBACK] krx_holidays.json not found; "
                "using empty holiday set (weekday-only heuristic)"
            )
            _KRX_CALENDAR_FALLBACK_LOGGED = True
        _KRX_HOLIDAYS_CACHE = set()
        return set()
    except Exception as exc:
        if not _KRX_CALENDAR_FALLBACK_LOGGED:
            logger.info(
                "[TIME][KRX][CALENDAR_FALLBACK] failed to load krx_holidays.json: %s; "
                "using empty holiday set",
                exc,
            )
            _KRX_CALENDAR_FALLBACK_LOGGED = True
        _KRX_HOLIDAYS_CACHE = set()
        return set()


def _reload_krx_holidays() -> set[date]:
    """캐시를 무효화하고 재로드한다 (주로 테스트용)."""
    global _KRX_HOLIDAYS_CACHE
    global _KRX_CALENDAR_FALLBACK_LOGGED
    _KRX_HOLIDAYS_CACHE = None
    _KRX_CALENDAR_FALLBACK_LOGGED = False
    return _load_krx_holidays()


def is_krx_trading_day(d: date | str) -> bool:
    """
    주어진 날짜가 KRX 거래일인지 판정한다.

    판정 순서:
      1. 주말 → 거래일 아님
      2. config/krx_holidays.json 휴장일 → 거래일 아님
      3. pykrx 결과 우선 (성공 시)
      4. pykrx 실패 → config fallback 결과 사용 (fail-close)

    Returns:
        True  → KRX 거래일
        False → 주말 또는 휴장일
    """
    if isinstance(d, str):
        d = date.fromisoformat(d)

    # 1. 주말 체크
    if d.weekday() >= 5:
        logger.debug("[TIME][KRX][TRADING_DAY] date=%s is_trading_day=0 reason=WEEKEND", d.isoformat())
        return False

    # 2. config fallback 휴장일 체크
    holidays = _load_krx_holidays()
    if d in holidays:
        logger.info(
            "[TIME][KRX][HOLIDAY] date=%s reason=KRX_HOLIDAY source=config",
            d.isoformat(),
        )
        return False

    # 3. pykrx 시도
    resolved, err_type = _safe_get_nearest_business_day_in_a_week(d.strftime("%Y%m%d"), prev=True)
    if err_type is None and resolved is not None:
        try:
            pykrx_date = date.fromisoformat(f"{resolved[:4]}-{resolved[4:6]}-{resolved[6:8]}")
            is_trading = pykrx_date == d
            if not is_trading:
                logger.info(
                    "[TIME][KRX][HOLIDAY] date=%s reason=PYKRX_NOT_TRADING pykrx_prev=%s",
                    d.isoformat(),
                    pykrx_date.isoformat(),
                )
            else:
                logger.debug(
                    "[TIME][KRX][TRADING_DAY] date=%s is_trading_day=1 source=pykrx",
                    d.isoformat(),
                )
            return is_trading
        except (ValueError, IndexError):
            pass

    # 4. pykrx 실패 → config fallback 결과 사용 (d가 holidays에 없으면 거래일로 판정)
    # 이미 config 체크를 했으므로 여기까지 오면 config에 없는 평일 → 거래일
    logger.info(
        "[TIME][KRX][CALENDAR_FALLBACK] date=%s pykrx_err=%s using_config_fallback=1",
        d.isoformat(),
        err_type or "UNKNOWN",
    )
    logger.debug("[TIME][KRX][TRADING_DAY] date=%s is_trading_day=1 source=config_fallback", d.isoformat())
    return True


def resolve_prev_krx_trading_day(run_date: date | str, *, max_lookback: int = 14) -> date:
    """
    기준일 직전 KRX 실제 거래일을 반환한다.

    - 주말 스킵
    - config/krx_holidays.json 휴장일 스킵
    - pykrx 사용 가능 시 pykrx 결과 우선

    Args:
        run_date: 기준 실행일 (이 날 포함하지 않고 직전 거래일 반환)
        max_lookback: 최대 역방향 탐색 일수 (기본 14일)

    Returns:
        직전 KRX 거래일 (date 객체)

    Raises:
        RuntimeError: max_lookback 이내에 거래일을 찾지 못한 경우
    """
    if isinstance(run_date, str):
        run_date = date.fromisoformat(run_date)

    skipped: list[str] = []
    candidate = run_date - timedelta(days=1)

    for _ in range(max_lookback):
        if is_krx_trading_day(candidate):
            logger.info(
                "[TIME][KRX][PREV_TRADING_DAY] run_date=%s prev=%s skipped=%s",
                run_date.isoformat(),
                candidate.isoformat(),
                ",".join(skipped) if skipped else "none",
            )
            return candidate
        skipped.append(candidate.isoformat())
        candidate -= timedelta(days=1)

    raise RuntimeError(
        f"[TIME][KRX][CALENDAR_FAIL_CLOSE] "
        f"could not find prev trading day within {max_lookback} days of {run_date}"
    )


def now_kst() -> datetime:
    """현재 KST 시각을 반환."""
    return datetime.now(tz=KST)


def week_monday(d: date | datetime | str) -> date:
    """
    주어진 날짜가 속한 주의 월요일을 반환.
    
    Args:
        d: KST 기준 날짜 (date/datetime/"YYYY-MM-DD" 형식)
    
    Returns:
        해당 주의 월요일 (date 객체)
    
    Examples:
        >>> week_monday(date(2026, 1, 30))  # 목요일
        date(2026, 1, 27)  # 월요일
        >>> week_monday("2026-01-30")  # 문자열도 지원
        date(2026, 1, 27)
    """
    if d is None:
        raise ValueError("week_monday: d is None")

    # normalize string -> date
    if isinstance(d, str):
        s = d.strip()
        # allow full ISO datetime too
        if "T" in s:
            d = datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        else:
            d = date.fromisoformat(s)

    # normalize datetime -> date
    if isinstance(d, datetime):
        d = d.date()

    if not isinstance(d, date):
        raise TypeError(f"week_monday: unsupported type {type(d)} value={d}")

    return d - timedelta(days=d.weekday())


def is_trading_weekday(ts: datetime) -> bool:
    # Mon=0 ... Sun=6
    return ts.weekday() < 5


def is_trading_day(ts: datetime | None = None) -> bool:
    """주말을 제외한 기본 거래일 여부를 판정.
    FORCE_TRADING_DAY=1 이면 강제로 True 반환 (테스트용)
    """

    ts = ts or now_kst()

    # 🔥 강제 거래일 테스트 모드
    if os.getenv("FORCE_TRADING_DAY") == "1":
        logger.warning(
            "[TIME_UTILS] FORCE_TRADING_DAY=1 → 비거래일 체크 우회 (%s)",
            ts.date(),
        )
        return True

    return is_trading_weekday(ts)


def is_trading_window(ts: datetime | None = None) -> bool:
    """당일 장중(09:00~15:30) 여부."""

    ts = ts or now_kst()

    # 거래일 여부도 동일하게 FORCE_TRADING_DAY 영향 받음
    if not is_trading_day(ts):
        return False

    return MARKET_OPEN <= ts.time() <= MARKET_CLOSE


def calc_market_window_kst(dt: datetime) -> str:
    """
    Returns one of: preopen, morning, day, close, after
    IMPORTANT: If not trading weekday => 'after' (weekend guard)
    """
    if not is_trading_weekday(dt):
        return "after"

    preopen_start = time.fromisoformat(os.getenv("PB1_PREOPEN_START", "08:45"))
    preopen_end = time.fromisoformat(os.getenv("PB1_PREOPEN_END", "09:00"))

    t = dt.time()
    if preopen_start <= t < preopen_end:
        return "preopen"
    if preopen_end <= t < time(10, 30):
        return "morning"
    if time(10, 30) <= t < time(15, 15):
        return "day"
    if time(15, 15) <= t <= time(15, 30):
        return "close"
    return "after"


def is_market_open_kst(dt: datetime | None = None) -> bool:
    """
    장중 여부 판단 (AUTO 모드 결정용).
    
    Returns:
        True: 장중 (09:00~15:20, 월~금)
        False: 장외 (주말, 장시작 전, 장마감 후)
    """
    dt = dt or now_kst()
    
    # 거래일 여부 확인
    if not is_trading_weekday(dt):
        return False
    
    # 장중 시간 확인 (09:00~15:30)
    t = dt.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


def market_close_dt_kst(dt: datetime) -> datetime:
    """
    주어진 날짜의 장 마감 시각(15:30) 반환.
    
    Args:
        dt: KST 기준 datetime
    
    Returns:
        같은 날 15:30:00 KST
    """
    return dt.replace(hour=15, minute=30, second=0, microsecond=0)


def prev_business_day(d: date) -> date:
    """
    주어진 날짜의 이전 영업일(월~금) 반환.
    
    Args:
        d: 기준 날짜
    
    Returns:
        이전 영업일 (date 객체)
    
    Examples:
        >>> prev_business_day(date(2026, 2, 3))  # 화요일
        date(2026, 2, 2)  # 월요일
        >>> prev_business_day(date(2026, 2, 1))  # 일요일
        date(2026, 1, 31)  # 금요일
        >>> prev_business_day(date(2026, 2, 2))  # 월요일
        date(2026, 1, 31)  # 금요일
    """
    prev = d - timedelta(days=1)
    
    # 주말이면 금요일까지 거슬러 올라감
    while prev.weekday() >= 5:  # 토(5), 일(6)
        prev -= timedelta(days=1)
    
    return prev


def _fallback_previous_or_same_weekday(d: date) -> date:
    """Fallback trading-day coercion when exchange calendar is unavailable."""
    resolved = d
    while resolved.weekday() >= 5:
        resolved -= timedelta(days=1)
    return resolved


def _fallback_previous_weekday(d: date) -> date:
    resolved = d - timedelta(days=1)
    while resolved.weekday() >= 5:
        resolved -= timedelta(days=1)
    return resolved


def _fallback_last_weekday_scan(d: date, *, lookback_days: int = 7) -> date:
    for days_back in range(1, max(lookback_days, 1) + 1):
        candidate = d - timedelta(days=days_back)
        if candidate.weekday() < 5:
            return candidate
    return _fallback_previous_weekday(d)


def _log_pykrx_fail_once(*, scope: str, subject: str, fallback: str, error: Exception | None = None, err_type: str | None = None) -> None:
    error_name = err_type or type(error).__name__
    signature = (scope, subject, error_name)
    if signature in _PYKRX_WARNED_SIGNATURES:
        return
    _PYKRX_WARNED_SIGNATURES.add(signature)
    logger.warning(
        "[TIME][TRADING_DAY][PYKRX_FAIL] %s fallback=%s err_type=%s",
        subject,
        fallback,
        error_name,
    )


@contextmanager
def _suppress_noisy_external_loggers():
    states: list[tuple[logging.Logger, bool, int]] = []
    try:
        for name in _NOISY_EXTERNAL_LOGGERS:
            ext_logger = logging.getLogger(name)
            states.append((ext_logger, ext_logger.disabled, ext_logger.level))
            ext_logger.disabled = True
        yield
    finally:
        for ext_logger, disabled, level in states:
            ext_logger.disabled = disabled
            ext_logger.setLevel(level)


def _safe_get_nearest_business_day_in_a_week(date_str: str, *, prev: bool = True) -> tuple[str | None, str | None]:
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    previous_disable_level = logging.root.manager.disable
    try:
        with _suppress_noisy_external_loggers(), redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            logging.disable(logging.CRITICAL)
            from pykrx.stock import get_nearest_business_day_in_a_week

            resolved = get_nearest_business_day_in_a_week(date_str, prev=prev)
        resolved_text = str(resolved).strip() if resolved is not None else ""
        if len(resolved_text) < 8:
            return None, "IndexError"
        return resolved_text[:8], None
    except IndexError:
        return None, "IndexError"
    except Exception as exc:
        return None, type(exc).__name__
    finally:
        logging.disable(previous_disable_level)


def _resolve_pykrx_previous_or_same(d: date) -> date | None:
    cache_key = d.isoformat()
    if cache_key in _PYKRX_PREV_OR_SAME_CACHE:
        return _PYKRX_PREV_OR_SAME_CACHE[cache_key]

    # 토/일은 PyKRX 호출 전에 조기 반환 – IndexError/PYKRX_FAIL 방지
    if d.weekday() >= 5:
        logger.info("[TIME][TRADING_DAY][WEEKEND] date=%s is_trading_day=0", d.isoformat())
        result = _fallback_previous_or_same_weekday(d)
        _PYKRX_PREV_OR_SAME_CACHE[cache_key] = result
        return result

    resolved, err_type = _safe_get_nearest_business_day_in_a_week(d.strftime("%Y%m%d"), prev=True)
    if not resolved:
        _log_pykrx_fail_once(
            scope="resolve_pykrx_previous_or_same",
            subject=f"date={d.isoformat()}",
            fallback="weekday_heuristic",
            err_type=err_type or "PYKRX_UNKNOWN_ERROR",
        )
        resolved_date = None
    else:
        try:
            resolved_date = date.fromisoformat(f"{resolved[:4]}-{resolved[4:6]}-{resolved[6:8]}")
        except (IndexError, ValueError) as exc:
            _log_pykrx_fail_once(
                scope="resolve_pykrx_previous_or_same",
                subject=f"date={d.isoformat()}",
                fallback="weekday_heuristic",
                err_type=type(exc).__name__,
            )
            resolved_date = None

    _PYKRX_PREV_OR_SAME_CACHE[cache_key] = resolved_date
    return resolved_date


def coerce_to_previous_trading_day(
    d: date,
    *,
    exchange: str = "KRX",
    trading_day_resolver: Callable[[date, str], date] | None = None,
) -> date:
    """주어진 날짜를 이전 또는 동일 거래일로 보정한다."""
    if trading_day_resolver is not None:
        return trading_day_resolver(d, exchange)

    heuristic = _fallback_previous_or_same_weekday(d)
    normalized_exchange = (exchange or "KRX").strip().upper()
    if normalized_exchange == "KRX":
        _resolve_pykrx_previous_or_same(d)
    return heuristic


def is_trading_date(
    d: date,
    *,
    exchange: str = "KRX",
    trading_day_resolver: Callable[[date, str], date] | None = None,
) -> bool:
    """특정 날짜가 거래일인지 판정한다."""
    if trading_day_resolver is not None:
        return trading_day_resolver(d, exchange) == d

    normalized_exchange = (exchange or "KRX").strip().upper()
    if normalized_exchange == "KRX":
        return is_krx_trading_day(d)

    # non-KRX: weekday heuristic
    return d.weekday() < 5


def resolve_prev_trading_day(
    run_date: date,
    *,
    exchange: str = "KRX",
    trading_day_resolver: Callable[[date, str], date] | None = None,
) -> date:
    """기준 실행일 직전 KRX 거래일을 반환한다."""
    candidate = run_date - timedelta(days=1)
    if trading_day_resolver is not None:
        return coerce_to_previous_trading_day(
            candidate,
            exchange=exchange,
            trading_day_resolver=trading_day_resolver,
        )

    normalized_exchange = (exchange or "KRX").strip().upper()
    if normalized_exchange == "KRX":
        # KRX canonical resolver: 휴장일 + 주말 모두 스킵
        return resolve_prev_krx_trading_day(run_date)

    # non-KRX: weekday heuristic
    return _fallback_last_weekday_scan(run_date)


def resolve_trade_readiness_as_of(
    *,
    run_date: date,
    market_window: str,
    exchange: str = "KRX",
    candidate_as_of: date | None = None,
    trading_day_resolver: Callable[[date, str], date] | None = None,
) -> dict[str, date | str | bool]:
    """Trade readiness/prep fallback에서 사용할 공통 as_of 정책을 반환한다."""
    window = (market_window or "").strip().lower()
    calendar_prev = run_date - timedelta(days=1)
    requested_as_of = candidate_as_of or calendar_prev
    trading_windows = {"preopen", "morning", "intraday", "open", "session", "day"}
    calendar_prev_is_trading_day = is_trading_date(
        calendar_prev,
        exchange=exchange,
        trading_day_resolver=trading_day_resolver,
    )

    if window in trading_windows:
        resolved_as_of = resolve_prev_trading_day(
            run_date,
            exchange=exchange,
            trading_day_resolver=trading_day_resolver,
        )
        reason = "PREV_TRADING_DAY"
    else:
        resolved_as_of = coerce_to_previous_trading_day(
            requested_as_of,
            exchange=exchange,
            trading_day_resolver=trading_day_resolver,
        )
        reason = "PREV_TRADING_DAY" if resolved_as_of != requested_as_of else "REQUESTED_AS_OF"

    return {
        "run_date": run_date,
        "calendar_prev": calendar_prev,
        "requested_as_of": requested_as_of,
        "resolved_as_of": resolved_as_of,
        "reason": reason,
        "is_calendar_prev_trading_day": calendar_prev_is_trading_day,
    }


def resolve_derived_as_of(now: datetime | None = None, cli_as_of: date | str | None = None) -> date:
    """
    Trade에서 사용할 derived 데이터의 as_of 날짜를 결정.
    
    장중 매매는 항상 "전일 종가 기반 derived"를 사용해야 하므로,
    현재 시각과 무관하게 전일 영업일을 반환한다.
    
    Args:
        now: 현재 시각 (없으면 now_kst() 사용)
    
    Returns:
        전일 영업일 (date 객체)
    
    Examples:
        >>> # 2026-02-10 (화) 장중 -> 2026-02-09 (월) derived 사용
        >>> resolve_derived_as_of(datetime(2026, 2, 10, 10, 0, tzinfo=KST))
        date(2026, 2, 9)
        
        >>> # 2026-02-10 (화) 새벽 -> 2026-02-09 (월) derived 사용
        >>> resolve_derived_as_of(datetime(2026, 2, 10, 3, 0, tzinfo=KST))
        date(2026, 2, 9)
    
    Rationale:
        - prep_runner는 전일 종가 기반으로 derived를 생성 (PREV_TRADING_DAY)
        - trade는 장중에 "오늘 종가"가 없으므로 전일 derived를 사용해야 함
        - 일관성: 장중/장외 무관하게 전일 영업일 사용
    """
    now = now or now_kst()

    if cli_as_of is not None:
        if isinstance(cli_as_of, date):
            derived_as_of = cli_as_of
        else:
            raw = str(cli_as_of).strip()
            if not raw:
                raise ValueError("cli_as_of is empty")
            try:
                derived_as_of = date.fromisoformat(raw)
            except ValueError as exc:
                raise ValueError(f"invalid cli_as_of format: {raw}") from exc
        logger.info(
            "[ASOF][RESOLVE] now=%s derived_as_of=%s reason=CLI_AS_OF",
            now.isoformat(),
            derived_as_of.isoformat(),
        )
        return derived_as_of

    as_of_override = (os.getenv("AS_OF_OVERRIDE") or "").strip()
    if as_of_override:
        try:
            derived_as_of = date.fromisoformat(as_of_override)
        except ValueError as exc:
            raise ValueError(f"invalid AS_OF_OVERRIDE format: {as_of_override}") from exc
        logger.info(
            "[ASOF][RESOLVE] now=%s derived_as_of=%s reason=AS_OF_OVERRIDE",
            now.isoformat(),
            derived_as_of.isoformat(),
        )
        return derived_as_of

    today = now.date()
    
    # 전일 거래일 계산 (주말/휴일 보정)
    derived_as_of = resolve_prev_trading_day(today)
    
    logger.debug(
        "[ASOF][RESOLVE] now=%s today=%s derived_as_of=%s reason=INTRADAY_USE_PREV_CLOSE",
        now.isoformat(),
        today.isoformat(),
        derived_as_of.isoformat(),
    )
    
    return derived_as_of


def resolve_trade_context(
    now: datetime | None = None,
    requested_as_of: date | str | None = None,
    trade_date: date | str | None = None,
    env: str = "practice",
) -> dict[str, str | bool]:
    now = now or now_kst()
    if now.tzinfo is None:
        now = now.replace(tzinfo=KST)
    resolved_trade_date = trade_date if isinstance(trade_date, date) else date.fromisoformat(str(trade_date)) if trade_date else now.date()
    calendar_prev = resolve_prev_trading_day(resolved_trade_date)
    resolved_as_of = resolve_derived_as_of(now, cli_as_of=requested_as_of)
    if requested_as_of is not None:
        source = "requested_as_of"
        reason = "REQUESTED_AS_OF_LOCKED"
    elif (os.getenv("AS_OF_OVERRIDE") or "").strip():
        source = "env_override"
        reason = "AS_OF_OVERRIDE"
    else:
        source = "intraday_prev_close"
        reason = "INTRADAY_USE_PREV_CLOSE"
    return {
        "trade_date": resolved_trade_date.isoformat(),
        "as_of": resolved_as_of.isoformat(),
        "calendar_prev": calendar_prev.isoformat(),
        "reason": reason,
        "source": source,
        "env": str(env or "practice").strip().lower(),
        "is_locked": True,
    }


class AsOfContext:
    """
    Trade 엔진 입력의 as_of 컨텍스트를 단일 구조로 통일.
    
    - trade_date: 오늘 거래일 (예: 2026-02-23)
    - requested_as_of: derived_as_of (요청 기준, 예: 2026-02-20)
    - actual_as_of: DB/폴백 확정 (반드시 사용, 예: 2026-02-19) ✅
    - reason: override / intraday_use_prev_close / fallback 등
    """
    
    def __init__(
        self,
        trade_date: date | str,
        requested_as_of: date | str,
        actual_as_of: date | str,
        reason: str = "default",
    ):
        """
        Args:
            trade_date: 오늘 거래일 (str="YYYY-MM-DD" 또는 date)
            requested_as_of: 요청 기준 (watchlist 로드 전 기준)
            actual_as_of: 실제 사용 (DB/watchlist에서 확정)
            reason: 컨텍스트 생성 이유
        """
        self.trade_date = self._to_date(trade_date)
        self.requested_as_of = self._to_date(requested_as_of)
        self.actual_as_of = self._to_date(actual_as_of)
        self.reason = reason or "default"
        
        # 간단한 검증
        if self.actual_as_of > self.trade_date:
            logger.warning(
                "[AsOfContext][VALIDATION] actual_as_of(%s) > trade_date(%s) - 미래값 사용 주의",
                self.actual_as_of.isoformat(),
                self.trade_date.isoformat(),
            )
    
    @staticmethod
    def _to_date(d: date | str) -> date:
        if isinstance(d, date):
            return d
        if isinstance(d, str):
            s = d.strip()
            if "T" in s:
                return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
            return date.fromisoformat(s)
        raise TypeError(f"AsOfContext: expected date|str, got {type(d)}")
    
    def to_dict(self) -> dict:
        """dict로 변환 (로깅/전달용)."""
        return {
            "trade_date": self.trade_date.isoformat(),
            "requested_as_of": self.requested_as_of.isoformat(),
            "actual_as_of": self.actual_as_of.isoformat(),
            "reason": self.reason,
        }
    
    def __repr__(self) -> str:
        return (
            f"AsOfContext(trade_date={self.trade_date.isoformat()}, "
            f"requested={self.requested_as_of.isoformat()}, "
            f"actual={self.actual_as_of.isoformat()}, "
            f"reason={self.reason})"
        )
