import json, time
import pytest
from trader.us.runner.tick_process import run_tick_in_process, TickProcessTimeout
from trader.us.execution.tick_context import TickExecutionContext

def _wait_for_cancel(tick_cancellation_event=None):
    while not tick_cancellation_event.is_set(): time.sleep(.01)
    time.sleep(10)

def test_timeout_child_is_joined_and_event_is_delivered():
    with pytest.raises(TickProcessTimeout) as caught:
        run_tick_in_process(_wait_for_cancel, kwargs={}, timeout_sec=.05, terminate_grace_sec=.05)
    assert caught.value.result['process_alive'] is False

def test_durable_state_fences_submit(tmp_path):
    state=tmp_path/'state.json'; state.write_text(json.dumps({'state':'CANCELLING','session_run_id':'r','session_generation':1,'active_tick_id':'t'}))
    ctx=TickExecutionContext('2026-07-16','am','r',1,'t',active_session_state_path=str(state))
    assert not ctx.broker_submit_allowed()
