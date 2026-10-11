"""US Minervini proof must use completed real OHLCV, never ATR proxy alone."""
from datetime import date, timedelta

from trader.us.candidate_pool_builder import _compute_verified_us_minervini_vcp
from trader.us.watchlist_builder import _select_entry_style


def _daily_fixture():
    base = date(2025, 1, 1)
    rows = []
    for i in range(260):
        if i < 180:
            close = 65.0 + 0.2 * i
        else:
            close = 102.0 if i >= 255 else 101.0
        j = max(0, i - 180)
        range_ratio = (0.080, 0.054, 0.034, 0.018)[min(3, j // 20)]
        high = close + 0.1
        low = high - range_ratio * close
        rows.append({
            "xymd": (base + timedelta(days=i)).strftime("%Y%m%d"),
            "clos": close,
            "high": high,
            "low": low,
            "tvol": 100 if i >= 245 else 2000,
        })
    return rows


def test_us_minervini_detects_real_contraction_trend_and_pivot():
    proof = _compute_verified_us_minervini_vcp(_daily_fixture())
    assert proof["vcp_evidence_source"] == "completed_daily_ohlcv"
    assert proof["vcp_pass"] is True, proof
    assert proof["trend_template_pass"] is True, proof
    assert proof["pivot_price"] > 0
    row = {**proof, "close": 102.0}
    assert _select_entry_style(
        row, pb1_score=0.55, momentum_score=0.52,
        pullback_score=0.40, breakout_score=0.50, vcp_score=0.90,
    ) == "vcp"


def test_us_minervini_missing_history_fails_closed():
    proof = _compute_verified_us_minervini_vcp(_daily_fixture()[:180])
    assert proof["vcp_pass"] is False
    assert proof["trend_template_pass"] is False
    assert proof["vcp_evidence_status"] == "insufficient_history"


def test_us_minervini_missing_ohlcv_volume_fails_closed():
    rows = _daily_fixture()
    rows[-1]["tvol"] = 0
    proof = _compute_verified_us_minervini_vcp(rows)
    assert proof["vcp_pass"] is False
    assert proof["vcp_evidence_status"] == "incomplete_ohlcv"


def test_us_minervini_non_contraction_rejects():
    rows = _daily_fixture()
    for row in rows[-80:]:
        row["low"] = row["high"] - 0.08 * row["clos"]
        row["tvol"] = 2000
    proof = _compute_verified_us_minervini_vcp(rows)
    assert proof["vcp_pass"] is False
