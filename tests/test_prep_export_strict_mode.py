from __future__ import annotations

import pytest

from trader.prep_runner import _handle_export_consistency_failure


def test_prep_export_strict_mode_raises() -> None:
    with pytest.raises(RuntimeError, match="PREP_EXPORT_FINAL30_CONSISTENCY_FAILED"):
        _handle_export_consistency_failure(strict_export=True)


def test_prep_export_non_strict_mode_allows_continue() -> None:
    _handle_export_consistency_failure(strict_export=False)
