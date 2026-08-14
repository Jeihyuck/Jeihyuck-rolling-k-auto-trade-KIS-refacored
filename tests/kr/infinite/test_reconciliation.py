from datetime import date
from trader.kr.infinite.models import Status
from trader.kr.infinite.reconciliation import reconcile
from .conftest import active,pos
def test_partial_sell_remains_pending_then_completes():
 s=active(status=Status.EXIT_PENDING);assert reconcile(s,pos(qty=40),date(2026,8,14))[0].status==Status.EXIT_PENDING
 done,_=reconcile(s,pos(qty=0,avg=0),date(2026,8,14));assert done.status==Status.COMPLETE and done.last_exit_date==date(2026,8,14)
def test_unowned_and_complete_mismatch_freeze():
 assert reconcile(None,pos(qty=1),date(2026,8,14))[1]=="KR_INF_UNOWNED_EXISTING_POSITION"
 assert reconcile(active(status=Status.COMPLETE),pos(qty=1),date(2026,8,14))[0].status==Status.FROZEN
