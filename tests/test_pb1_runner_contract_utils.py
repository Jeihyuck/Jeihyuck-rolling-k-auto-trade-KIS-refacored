from trader.pb1_runner import _missing_scored_cols, _safe_flow_optional_missing
from trader.pb1_runner_contract_utils import missing_scored_cols, safe_flow_optional_missing


def test_pb1_runner_contract_utils_match_wrappers():
    cols = ["code", "score_final", "last_close"]
    assert _missing_scored_cols(cols) == missing_scored_cols(cols)
    assert _safe_flow_optional_missing(cols) == safe_flow_optional_missing(cols, flow_optional_cols=_safe_flow_optional_missing.__globals__.get("FLOW_OPTIONAL_COLS", []))
