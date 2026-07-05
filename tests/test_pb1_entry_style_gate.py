from trader.pb1_engine import PB1Engine, CandidateFeature


def _engine():
    e = PB1Engine.__new__(PB1Engine)
    e.require_volume = True
    e.phase = "entry"
    e.env = "practice"
    e.final30_locked = True
    e._precomputed_final30_map = {"012330": {}}
    e._get_current_price_for_entry = lambda code, features: None
    return e


def test_momentum_candidate_does_not_fail_on_pullback_band(monkeypatch):
    e = _engine()
    features = {
        "entry_style_selected": "MOMENTUM",
        "momentum_score": 75,
        "score_final": 70,
        "rs_percentile": 75,
        "atr_pct": 0.04,
        "close": 99500,
        "ma20": 100000,
        "ma50": 98000,
        "pullback_pct": 0.0,
        "vol_contraction": 1.5,
        "volu_contraction": 1.5,
    }
    ok, reasons, meta = e._evaluate_final30_entry_setup("012330", features, "KOSPI")
    assert ok is True
    assert "pullback_out_of_band" not in reasons
    assert meta["setup_source"] == "entry_style_momentum"


def test_pullback_candidate_keeps_existing_pb1_filter():
    e = _engine()
    features = {
        "entry_style_selected": "PULLBACK",
        "close": 95,
        "ma20": 100,
        "ma50": 90,
        "atr_pct": 0.04,
        "vol_contraction": 0.5,
        "volu_contraction": 0.5,
        "volume_missing": False,
    }
    ok, reasons, _ = e._evaluate_final30_entry_setup("000001", features, "KOSPI")
    assert ok is False
    assert "close_below_ma20" in reasons


def test_minervini_bridge_promotes_without_ma20_reclaim(monkeypatch):
    monkeypatch.setenv("PB1_RELAX_BRIDGE_REQUIRE_MA20_RECLAIM", "0")
    monkeypatch.setenv("PB1_RELAX_BRIDGE_REQUIRE_CURRENT_PRICE", "0")
    e = _engine()
    cf = CandidateFeature(
        code="012330",
        market="KOSPI",
        features={"score_final": 80, "rs_percentile": 75, "atr_pct": 0.04, "ma20": 100, "close": 90, "value20": 4000000000},
        setup_ok=False,
        reasons=["close_below_ma20"],
        mode=1,
        mode_reasons=[],
    )
    out = e._activate_relax_bridge_candidates(
        [cf],
        setup_ok_count=0,
        scanner_passed_codes=set(),
        minervini_passed_codes={"012330"},
        order_allowed=True,
    )
    assert len(out) == 1
    assert out[0].setup_ok is True
    assert out[0].features["entry_reason"] == "ENTRY_MINERVINI_RELAX_BRIDGE"
