"""
시간/세션 유틸 (KST 기준)
- 09:00 시작 → 14:45 종료(강제청산 윈도우 14:40~14:45) 같은 전략 파라미터를 코드에서 쉽게 사용하도록 제공
- 한국 공휴일은 선택적으로 `trader/holidays_kr.json`(YYYY-MM-DD 배열)에서 로드. 없으면 주말만 휴장으로 간주.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Iterable, Optional, Set, Dict, Any
import json
import logging

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

log = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul") if ZoneInfo else None
HOLIDAYS_FILE = Path(__file__).parent / "holidays_kr.json"


# -----------------------------
# 파싱/포맷 유틸
# -----------------------------

def kst_now() -> datetime:
    """KST 현재 시각(aware) 반환."""
    if KST is None:
        return datetime.now()
    return datetime.now(KST)


def to_kst(dt: datetime) -> datetime:
    """임의 tz 의 datetime을 KST로 변환. naive면 KST로 가정."""
    if KST is None:
        return dt
    if dt.tzinfo is None:
        return dt.replace(tzinfo=KST)
    return dt.astimezone(KST)


def hhmm(dt: datetime) -> str:
    """datetime → "HH:MM"."""
    return to_kst(dt).strftime("%H:%M")


def parse_hhmm(s: str) -> time:
    """"14:45" 같은 문자열을 time으로."""
    h, m = s.strip().split(":")
    return time(hour=int(h), minute=int(m))


def minutes_of_day(t: time) -> int:
    return t.hour * 60 + t.minute


def minutes_of_hhmm(s: str) -> int:
    return minutes_of_day(parse_hhmm(s))


# -----------------------------
# 한국 휴일 로딩 (선택)
# -----------------------------

def load_holidays(path: Path = HOLIDAYS_FILE) -> Set[date]:
    """
    trader/holidays_kr.json 에서 YYYY-MM-DD 문자열 배열을 로드.
    파일이 없으면 빈 집합.
    """
    try:
        if path.exists():
            arr = json.loads(path.read_text(encoding="utf-8"))
            out: Set[date] = set()
            for s in arr:
                try:
                    y, m, d = (int(x) for x in s.split("-"))
                    out.add(date(y, m, d))
                except Exception:
                    continue
            return out
    except Exception as e:
        log.warning("[holidays] load error: %s", e)
    return set()


# -----------------------------
# 장개장/세션 판정
# -----------------------------

def is_business_day(d: date, holidays: Optional[Set[date]] = None) -> bool:
    """주말이 아니고, (있다면) 휴일 목록에 없으면 영업일."""
    if d.weekday() >= 5:  # 5=토, 6=일
        return False
    if holidays and d in holidays:
        return False
    return True


@dataclass(frozen=True)
class TimeRules:
    trade_start_hhmm: str  # 예: "09:00"
    trade_end_hhmm: str    # 예: "14:45"
    force_start_hhmm: str  # 예: "14:40"
    force_end_hhmm: str    # 예: "14:45"

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "TimeRules":
        tr = (cfg or {}).get("time_rules", {})
        return cls(
            trade_start_hhmm=str(tr.get("trade_start_hhmm", "09:00")),
            trade_end_hhmm=str(tr.get("trade_end_hhmm", "14:45")),
            force_start_hhmm=str(tr.get("sell_force_window", {}).get("start_hhmm", "14:40")),
            force_end_hhmm=str(tr.get("sell_force_window", {}).get("end_hhmm", "14:45")),
        )


def _within_hhmm(now: datetime, start_hhmm: str, end_hhmm: str, inclusive_end: bool = False) -> bool:
    """
    now(KST) 가 [start, end) 또는 [start, end] 내인지 판정.
    inclusive_end=True면 end 포함.
    """
    k = to_kst(now)
    cur = minutes_of_day(time(k.hour, k.minute))
    s = minutes_of_hhmm(start_hhmm)
    e = minutes_of_hhmm(end_hhmm)
    if inclusive_end:
        return s <= cur <= e
    return s <= cur < e


def is_within_trading_session(now: Optional[datetime], rules: TimeRules) -> bool:
    """정규 매매 세션(예: 09:00~14:45) 내인지."""
    n = now or kst_now()
    return _within_hhmm(n, rules.trade_start_hhmm, rules.trade_end_hhmm, inclusive_end=False)


def is_within_force_sell_window(now: Optional[datetime], rules: TimeRules) -> bool:
    """강제청산 윈도우(예: 14:40~14:45) 내인지 (끝 포함)."""
    n = now or kst_now()
    return _within_hhmm(n, rules.force_start_hhmm, rules.force_end_hhmm, inclusive_end=True)


def is_market_open_today(now: Optional[datetime] = None, holidays: Optional[Set[date]] = None) -> bool:
    """오늘이 영업일인지(휴일/주말 제외)."""
    n = to_kst(now or kst_now())
    return is_business_day(n.date(), holidays or load_holidays())


def seconds_until_hhmm(target_hhmm: str, now: Optional[datetime] = None) -> int:
    """지금부터 target_hhmm(KST)까지 남은 초. 이미 지났으면 0."""
    n = to_kst(now or kst_now())
    t = parse_hhmm(target_hhmm)
    tgt = n.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
    delta = (tgt - n).total_seconds()
    return int(delta) if delta > 0 else 0


def next_wakeup_seconds(rules: TimeRules, tick_seconds: int = 60, now: Optional[datetime] = None) -> int:
    """루프 슬립 길이 결정: 세션 전/중/후에 따라 대기 시간을 합리적으로 조절."""
    n = to_kst(now or kst_now())
    if _within_hhmm(n, "00:00", rules.trade_start_hhmm):
        # 장 전: 09:00까지 남은 시간(최대 60초 간격으로 깨어나도록 캡)
        remain = seconds_until_hhmm(rules.trade_start_hhmm, n)
        return min(remain, tick_seconds)
    if is_within_trading_session(n, rules):
        return tick_seconds
    if is_within_force_sell_window(n, rules):
        return 1  # 강제청산 창에서는 빠른 루프 권장
    # 세션 후: 종료
    return 0


# -----------------------------
# 간단 상태 질의 (트레이더에서 사용)
# -----------------------------

def status(now: Optional[datetime], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    현재 상태 요약을 dict로 반환.
    {
      'now': '14:12', 'business_day': True, 'session': 'pre'|'live'|'force'|'closed'
    }
    """
    n = to_kst(now or kst_now())
    rules = TimeRules.from_config(cfg)
    biz = is_market_open_today(n)
    if not biz:
        sess = "closed"
    else:
        if is_within_trading_session(n, rules):
            sess = "live"
        elif is_within_force_sell_window(n, rules):
            sess = "force"
        elif _within_hhmm(n, "00:00", rules.trade_start_hhmm):
            sess = "pre"
        else:
            sess = "closed"
    return {
        "now": hhmm(n),
        "business_day": biz,
        "session": sess,
        "rules": {
            "trade_start_hhmm": rules.trade_start_hhmm,
            "trade_end_hhmm": rules.trade_end_hhmm,
            "force_start_hhmm": rules.force_start_hhmm,
            "force_end_hhmm": rules.force_end_hhmm,
        },
    }


# -----------------------------
# 테스트 실행 (로컬 확인용)
# -----------------------------
if __name__ == "__main__":
    import pprint
    sample_cfg = {
        "time_rules": {
            "trade_start_hhmm": "09:00",
            "trade_end_hhmm": "14:45",
            "sell_force_window": {"start_hhmm": "14:40", "end_hhmm": "14:45"},
        }
    }
    pprint.pp(status(kst_now(), sample_cfg))
