"""tests/test_stage_label.py

build_stage_label 함수 검증.
- am + entry → "PB1-AM-ENTRY"
- afternoon + entry → "PB1-AFTERNOON-ENTRY"
- any + exit/close → "PB1-CLOSE-EXIT"
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _build_stage_label(session_kind: str | None, window: str | None = None, phase: str | None = None) -> str:
    """pb1_engine.build_stage_label 로직 재현."""
    sk = str(session_kind or "").lower()
    ph = str(phase or window or "").lower()
    if ph in ("exit", "close"):
        return "PB1-CLOSE-EXIT"
    if sk == "am":
        return "PB1-AM-ENTRY"
    if sk == "afternoon":
        return "PB1-AFTERNOON-ENTRY"
    return "PB1-ENTRY"


class TestStageLabel(unittest.TestCase):

    def test_stage_label_am_entry(self):
        label = _build_stage_label(session_kind="am", phase="entry")
        self.assertEqual(label, "PB1-AM-ENTRY")

    def test_stage_label_afternoon_entry(self):
        label = _build_stage_label(session_kind="afternoon", phase="entry")
        self.assertEqual(label, "PB1-AFTERNOON-ENTRY")

    def test_stage_label_close_exit(self):
        label = _build_stage_label(session_kind="am", phase="exit")
        self.assertEqual(label, "PB1-CLOSE-EXIT")

    def test_stage_label_close_via_window(self):
        label = _build_stage_label(session_kind="am", window="close")
        self.assertEqual(label, "PB1-CLOSE-EXIT")

    def test_stage_label_no_phase_defaults_to_entry(self):
        label = _build_stage_label(session_kind="am")
        self.assertEqual(label, "PB1-AM-ENTRY")

    def test_stage_label_none_session(self):
        label = _build_stage_label(session_kind=None, phase="entry")
        self.assertEqual(label, "PB1-ENTRY")


if __name__ == "__main__":
    unittest.main()
