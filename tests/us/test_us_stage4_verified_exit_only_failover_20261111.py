"""No native broker I/O: verify the only safe Stage4 EXIT_ONLY handoff."""
import pytest

from trader.us.execution.stage4_failover import verified_stage4_exit_only_handoff


class Child:
    def __init__(self, exited=True):
        self.exited = exited

    def poll(self):
        return 0 if self.exited else None


@pytest.mark.parametrize("env,generation", [
    ("real", "g1"), ("practice", ""), ("practice", None),
])
def test_forbidden_scope_never_touches_stop(env, generation):
    def fail(*args, **kwargs):
        pytest.fail("NO_STOP_REQUEST_ON_INVALID_SCOPE")
    ok, _ = verified_stage4_exit_only_handoff(
        child=Child(), expected_generation=generation, env=env,
        stop_worker=fail, confirm_row_absent=fail,
    )
    assert not ok


def test_exact_stopped_generation_can_fallback():
    called = []
    ok, reason = verified_stage4_exit_only_handoff(
        child=Child(), expected_generation="g1",
        stop_worker=lambda child, **kw: called.append(kw) or True,
        confirm_row_absent=lambda **kw: pytest.fail("STOP_ALREADY_MARKED"),
    )
    assert (ok, reason) == (True, "WRITER_STOP_PROVEN")
    assert called == [{"expected_generation": "g1"}]


def test_exited_before_ready_requires_verified_missing_row():
    ok, _ = verified_stage4_exit_only_handoff(
        child=Child(), expected_generation="g1",
        stop_worker=lambda *args, **kwargs: False,
        confirm_row_absent=lambda **kwargs: kwargs == {
            "env": "practice", "expected_generation": "g1",
        },
    )
    assert ok


@pytest.mark.parametrize("stopped,exited,missing", [
    (False, True, False), (True, False, True), (False, False, True),
])
def test_ambiguous_handoff_is_never_authorized(stopped, exited, missing):
    ok, _ = verified_stage4_exit_only_handoff(
        child=Child(exited=exited), expected_generation="g1",
        stop_worker=lambda *args, **kwargs: stopped,
        confirm_row_absent=lambda **kwargs: missing,
    )
    assert not ok


def test_stop_exception_fails_closed():
    def broken(*args, **kwargs):
        raise TimeoutError("STAGE4_STOP_TIMEOUT")
    ok, reason = verified_stage4_exit_only_handoff(
        child=Child(), expected_generation="g1",
        stop_worker=broken,
        confirm_row_absent=lambda **kwargs: True,
    )
    assert not ok and reason == "WRITER_HANDOFF_ERROR_TIMEOUTERROR"
