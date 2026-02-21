from __future__ import annotations

from trader.minervini_filter import select_buyable_with_relax


def test_relax_progression_counts(monkeypatch):
    monkeypatch.setenv("MINERVINI_RS_MIN_PCTILE", "80")
    monkeypatch.setenv("MINERVINI_VCP_MIN_SCORE", "70")
    monkeypatch.setenv("ALLOW_RS_ONLY_WHEN_STRONG_TREND", "1")

    signals = {
        "regime_pass": True,
        "items": [
            {"code": "000001", "data_ok": True, "trend_pass": True, "atr_pass": True, "rs_pctile": 82, "vcp_score": 72},
            {"code": "000002", "data_ok": True, "trend_pass": True, "atr_pass": True, "rs_pctile": 81, "vcp_score": 71},
            {"code": "000003", "data_ok": True, "trend_pass": True, "atr_pass": True, "rs_pctile": 76, "vcp_score": 67},
            {"code": "000004", "data_ok": True, "trend_pass": True, "atr_pass": True, "rs_pctile": 75, "vcp_score": 65},
            {"code": "000005", "data_ok": True, "trend_pass": True, "atr_pass": True, "rs_pctile": 71, "vcp_score": 61},
            {"code": "000006", "data_ok": True, "trend_pass": True, "atr_pass": True, "rs_pctile": 70, "vcp_score": 60},
        ],
    }

    buyable, report = select_buyable_with_relax(
        signals=signals,
        min_buyable=5,
        relax_passes=3,
        rs_step=5,
        vcp_step=5,
        keep_trend=True,
    )

    assert report["pass_counts"]["pass0"] == 2
    assert report["pass_counts"]["pass1"] == 4
    assert report["pass_counts"]["pass2"] == 6
    assert report["relax_level_used"] == 2
    assert report["rs_cut_used"] == 70
    assert report["vcp_cut_used"] == 60
    assert report["final_buyable_count"] == 6
    assert len(buyable) == 6
