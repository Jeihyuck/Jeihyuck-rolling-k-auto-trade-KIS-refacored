"""거래일/거래 가능 시간 헬퍼."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(15, 20)


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
    """당일 장중(09:00~15:20) 여부."""

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
    if preopen_end <= t < time(10, 0):
        return "morning"
    if time(10, 0) <= t < time(15, 15):
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
    
    # 장중 시간 확인 (09:00~15:20)
    t = dt.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


def market_close_dt_kst(dt: datetime) -> datetime:
    """
    주어진 날짜의 장 마감 시각(15:15) 반환.
    
    Args:
        dt: KST 기준 datetime
    
    Returns:
        같은 날 15:15:00 KST
    """
    return dt.replace(hour=15, minute=15, second=0, microsecond=0)


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


def coerce_to_previous_trading_day(
    d: date,
    *,
    exchange: str = "KRX",
    trading_day_resolver: Callable[[date, str], date] | None = None,
) -> date:
    """주어진 날짜를 이전 또는 동일 거래일로 보정한다."""
    if trading_day_resolver is not None:
        return trading_day_resolver(d, exchange)

    normalized_exchange = (exchange or "KRX").strip().upper()
    if normalized_exchange == "KRX":
        try:
            from pykrx.stock import get_nearest_business_day_in_a_week

            resolved = get_nearest_business_day_in_a_week(d.strftime("%Y%m%d"), prev=True)
            return date.fromisoformat(f"{resolved[:4]}-{resolved[4:6]}-{resolved[6:8]}")
        except Exception as exc:
            logger.debug(
                "[TIME_UTILS][TRADING_DAY] exchange=%s date=%s fallback=weekday_only err=%s",
                normalized_exchange,
                d.isoformat(),
                exc,
            )

    return _fallback_previous_or_same_weekday(d)


def is_trading_date(
    d: date,
    *,
    exchange: str = "KRX",
    trading_day_resolver: Callable[[date, str], date] | None = None,
) -> bool:
    """특정 날짜가 거래일인지 판정한다."""
    return coerce_to_previous_trading_day(
        d,
        exchange=exchange,
        trading_day_resolver=trading_day_resolver,
    ) == d


def resolve_prev_trading_day(
    run_date: date,
    *,
    exchange: str = "KRX",
    trading_day_resolver: Callable[[date, str], date] | None = None,
) -> date:
    """기준 실행일 직전 거래일을 반환한다."""
    candidate = run_date - timedelta(days=1)
    return coerce_to_previous_trading_day(
        candidate,
        exchange=exchange,
        trading_day_resolver=trading_day_resolver,
    )


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
        "is_calendar_prev_trading_day": is_trading_date(
            calendar_prev,
            exchange=exchange,
            trading_day_resolver=trading_day_resolver,
        ),
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
