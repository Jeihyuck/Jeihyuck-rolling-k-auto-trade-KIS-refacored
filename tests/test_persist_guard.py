import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.botstate_sync import BotStatePersistError, evaluate_persist_guard


def test_persist_guard_hard_fail_on_dirty_stat():
    with pytest.raises(BotStatePersistError):
        evaluate_persist_guard(require_persist=True, dirty_by_stat=True)


def test_persist_guard_warn_on_empty_stat():
    result = evaluate_persist_guard(require_persist=True, dirty_by_stat=False)
    assert result == "warn"
