"""V3 verifies 5W1H executable SHA, not an unproved local Git checkout.

This helper has no network access and never changes any trading permission.
"""
import pytest

from trader.us.run_manifest import verified_execution_revision

EXEC_SHA = "a" * 40
OTHER_SHA = "b" * 40


@pytest.mark.parametrize("actual,session,expected,guard,verified", [
    (EXEC_SHA, EXEC_SHA, EXEC_SHA, True, EXEC_SHA),
    (EXEC_SHA.upper(), EXEC_SHA, EXEC_SHA, True, EXEC_SHA),
    (EXEC_SHA, EXEC_SHA, None, True, EXEC_SHA),
    (EXEC_SHA, OTHER_SHA, EXEC_SHA, True, ""),
    (EXEC_SHA, EXEC_SHA, OTHER_SHA, True, ""),
    (EXEC_SHA, EXEC_SHA, EXEC_SHA, False, ""),
    ("unknown", EXEC_SHA, EXEC_SHA, True, ""),
    ("", EXEC_SHA, EXEC_SHA, True, ""),
    (EXEC_SHA, EXEC_SHA, "unverified", True, ""),
])
def test_execution_audit_sha_requires_exact_runtime_and_prep_match(
    actual, session, expected, guard, verified,
):
    assert verified_execution_revision(
        actual_git_head=actual,
        session_revision=session,
        expected_revision=expected,
        revision_guard_ok=guard,
    ) == verified
