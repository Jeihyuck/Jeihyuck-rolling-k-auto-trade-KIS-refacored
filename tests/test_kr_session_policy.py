from datetime import date, datetime
from zoneinfo import ZoneInfo
from trader.kr.runner.session_policy import kr_am_policy, kr_prep_schedule_guard
KST=ZoneInfo('Asia/Seoul')

def test_kr_am_0855_waits_not_skip():
    d=kr_am_policy(datetime(2026,6,16,8,55,1,tzinfo=KST))
    assert d.action=='WAIT' and d.reason=='WAIT_UNTIL_TARGET' and d.wait_seconds>0

def test_kr_am_0905_proceeds():
    d=kr_am_policy(datetime(2026,6,16,9,5,0,tzinfo=KST))
    assert d.action=='PROCEED'

def test_kr_prep_0028_blocked():
    d=kr_prep_schedule_guard(datetime(2026,6,16,0,28,0,tzinfo=KST), allow_outside=False)
    assert d.action=='BLOCK' and d.exit_code==2

def test_kr_am_0028_too_early_fail():
    d=kr_am_policy(datetime(2026,6,16,0,28,0,tzinfo=KST))
    assert d.action=='FAIL' and d.reason=='TOO_EARLY_FAIL' and d.exit_code==2

def test_kr_am_0800_too_early_fail():
    d=kr_am_policy(datetime(2026,6,16,8,0,0,tzinfo=KST))
    assert d.action=='FAIL' and d.reason=='TOO_EARLY_FAIL'

def test_kr_am_wait_truncated_before_target():
    d=kr_am_policy(datetime(2026,6,16,8,55,1,tzinfo=KST), max_wait_seconds=10)
    assert d.action=='FAIL' and d.reason=='KR_AM_WAIT_TOO_LONG' and d.exit_code==2


def test_am_wait_too_long_returns_structured_failure(monkeypatch):
    from trader.kr.runner import trade_session_runner as runner

    monkeypatch.setenv("KR_AM_ENTRY_START_TIME", "09:00:05")
    monkeypatch.setenv("KR_AM_MAX_WAIT_SEC", "1")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("MARKET", "KR")
    monkeypatch.setenv("REGION", "KR")
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KRX")

    class Ctx:
        session = "am"
        trade_date = date(2026, 6, 18)
        expected_as_of = date(2026, 6, 17)
        env = "practice"
        market = "KR"
        now_kst = None
        artifact_valid = False
        artifact_reason = None
        balance_state = None
        close_forced = False

    def fake_wait_until_kr_am_target(*args, **kwargs):
        raise RuntimeError("KR_AM_WAIT_TOO_LONG")

    monkeypatch.setattr(runner, "wait_until_kr_am_target", fake_wait_until_kr_am_target)

    result = runner._guard_trade_session("am", Ctx())

    assert result["status"] == "FAIL"
    assert result["reason"] == "KR_AM_WAIT_TOO_LONG"
    assert result["exit_code"] == 2
