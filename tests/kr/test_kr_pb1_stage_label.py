from trader.kr.pb1.stage_label import _resolve_session_window_name, build_stage_label
from trader.pb1_engine import build_stage_label as facade_build_stage_label


def test_stage_label_module_matches_facade():
    assert build_stage_label(session_kind="am", phase="entry") == "PB1-AM-ENTRY"
    assert facade_build_stage_label(session_kind="am", phase="entry") == "PB1-AM-ENTRY"
    assert build_stage_label(session_kind="afternoon", phase="entry") == "PB1-AFTERNOON-ENTRY"
    assert facade_build_stage_label(session_kind="afternoon", phase="entry") == "PB1-AFTERNOON-ENTRY"
    assert build_stage_label(session_kind="am", phase="exit") == "PB1-CLOSE-EXIT"
    assert facade_build_stage_label(session_kind="am", window="close") == "PB1-CLOSE-EXIT"


def test_resolve_session_window_name_module_matches_expected_contract():
    assert _resolve_session_window_name(session_kind="am", raw_window_name="day") == "morning"
    assert _resolve_session_window_name(session_kind="am", raw_window_name="close") == "intraday"
    assert _resolve_session_window_name(session_kind="afternoon", raw_window_name="after") == "after"
    assert _resolve_session_window_name(session_kind=None, raw_window_name=None) == "day"
