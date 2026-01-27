import pytest
from unittest.mock import Mock, patch
from trader.run_context import RunContext
from trader.utils.ids import assert_uuid, new_run_id
from trader.db.repos import LedgerEventsRepo, ReconcileLogRepo
from uuid import uuid4


def test_new_run_id():
    run_id = new_run_id()
    assert_uuid(run_id)


def test_assert_uuid_valid():
    valid_uuid = str(uuid4())
    assert_uuid(valid_uuid)  # Should not raise


def test_assert_uuid_invalid():
    with pytest.raises(ValueError, match="run_id must be a valid UUID string"):
        assert_uuid("123")


def test_run_context_creation():
    ctx = RunContext.new(
        env="test",
        strategy="test_strategy",
        gh_run_number=123,
        git_sha="abc123",
        window="daily",
        dry_run=True
    )
    assert ctx.run_id
    assert_uuid(ctx.run_id)
    assert ctx.env == "test"
    assert ctx.strategy == "test_strategy"
    assert ctx.gh_run_number == 123
    assert ctx.git_sha == "abc123"
    assert ctx.window == "daily"
    assert ctx.dry_run is True


@patch('trader.db.repos.ensure_run_from_context')
def test_append_event_from_context_invalid_run_id(mock_ensure):
    from trader.db.repos import LedgerEventsRepo
    repo = LedgerEventsRepo(Mock())
    ctx = Mock()
    ctx.run_id = "invalid"
    ctx.env = "test"
    ctx.strategy = "test"

    with pytest.raises(ValueError):
        repo.append_event_from_context(ctx, "test_event", Mock())


@patch('trader.db.repos.ensure_run_from_context')
def test_append_log_from_context_invalid_run_id(mock_ensure):
    from trader.db.repos import ReconcileLogRepo
    repo = ReconcileLogRepo(Mock())
    ctx = Mock()
    ctx.run_id = "invalid"
    ctx.env = "test"
    ctx.strategy = "test"

    with pytest.raises(ValueError):
        repo.append_log_from_context(ctx, "test_action", Mock())


def test_gh_run_number_not_used_as_run_id():
    # Ensure gh_run_number is not used as run_id
    ctx = RunContext.new(env="test", strategy="test", gh_run_number=456)
    assert ctx.run_id != "456"
    assert ctx.gh_run_number == 456