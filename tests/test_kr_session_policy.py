from datetime import datetime
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
