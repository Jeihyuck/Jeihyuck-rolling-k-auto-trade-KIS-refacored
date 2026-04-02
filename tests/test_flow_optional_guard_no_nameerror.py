from __future__ import annotations

from trader import pb1_runner


def test_flow_optional_guard_avoids_nameerror() -> None:
    original = getattr(pb1_runner, "FLOW_OPTIONAL_COLS", None)
    if hasattr(pb1_runner, "FLOW_OPTIONAL_COLS"):
        delattr(pb1_runner, "FLOW_OPTIONAL_COLS")

    try:
        missing = pb1_runner._safe_flow_optional_missing(["code", "score_final"])
    finally:
        if original is not None:
            setattr(pb1_runner, "FLOW_OPTIONAL_COLS", original)

    assert missing == []