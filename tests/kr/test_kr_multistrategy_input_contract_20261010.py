"""Regression coverage for KR entry RS units and emergency universe venue provenance.

Scope is implementation correctness only: no strategy thresholds or sell policy
are changed. The formerly unlabeled bundled emergency seed must fail closed
until a verified code,market CSV replaces it.
"""
import pytest

from trader.pb1_engine import PB1Engine
from trader.universe import build


def test_emergency_universe_rejects_legacy_unlabeled_seed(monkeypatch):
    monkeypatch.setattr(
        build,
        "_load_seed_rows",
        lambda _path: [{"code": "036930", "name": None, "market": None}],
    )
    assert build._load_emergency_seed() is None


def test_emergency_universe_keeps_explicit_market_not_file_position(monkeypatch):
    # KOSDAQ precedes KOSPI deliberately: a positional split is unsafe.
    monkeypatch.setattr(
        build,
        "_load_seed_rows",
        lambda _path: [
            {"code": "036930", "name": "주성엔지니어링", "market": "KOSDAQ"},
            {"code": "042700", "name": "한미반도체", "market": "KOSPI"},
            {"code": "039030", "name": "이오테크닉스", "market": "KOSDAQ"},
        ],
    )
    result = build._load_emergency_seed()
    assert result is not None
    assert [(item["code"], item["market"]) for item in result["members"]] == [
        ("036930", "KOSDAQ"),
        ("042700", "KOSPI"),
        ("039030", "KOSDAQ"),
    ]
    assert [row["code"] for row in result["payload"]["selected_by_market"]["KOSDAQ"]] == [
        "036930", "039030",
    ]


def test_emergency_universe_rejects_partial_market_labels(monkeypatch):
    monkeypatch.setattr(
        build,
        "_load_seed_rows",
        lambda _path: [
            {"code": "005930", "market": "KOSPI"},
            {"code": "036930", "market": None},
        ],
    )
    assert build._load_emergency_seed() is None


def test_seed_reader_carries_explicit_market_label(tmp_path):
    file_path = tmp_path / "market_seed.csv"
    file_path.write_text(
        "code,market,name\n036930,KOSDAQ,주성엔지니어링\n042700,KOSPI,한미반도체\n",
        encoding="utf-8",
    )
    rows = build._load_seed_rows(file_path)
    assert [(row["code"], row["market"]) for row in rows] == [
        ("036930", "KOSDAQ"), ("042700", "KOSPI"),
    ]


@pytest.mark.parametrize(
    ("style", "ratio", "expected_pass"),
    [
        ("MOMENTUM", 0.85, True),
        ("MOMENTUM", 0.40, False),
        ("BREAKOUT", 0.80, True),
        ("BREAKOUT", 0.30, False),
        ("VCP", 0.80, True),
        ("VCP", 0.30, False),
        ("MOMENTUM", 85.0, True),  # already on 0..100 scale
    ],
)
def test_kr_style_gate_accepts_valid_percentile_units(monkeypatch, style, ratio, expected_pass):
    monkeypatch.setenv("PB1_STYLE_GATE_ENABLED", "1")
    monkeypatch.setenv("PB1_MOMENTUM_GATE_ENABLED", "1")
    monkeypatch.setenv("PB1_MOMENTUM_MIN_SCORE", "60")
    monkeypatch.setenv("PB1_MOMENTUM_MIN_FINAL_SCORE", "55")
    monkeypatch.setenv("PB1_MOMENTUM_MIN_RS", "60")
    monkeypatch.setenv("PB1_BREAKOUT_MIN_SCORE", "55")
    monkeypatch.setenv("PB1_BREAKOUT_MIN_RS", "55")
    monkeypatch.setenv("PB1_VCP_MIN_SCORE", "45")
    monkeypatch.setenv("PB1_VCP_MIN_RS", "55")
    engine = object.__new__(PB1Engine)
    engine.env = "practice"
    engine.require_volume = False
    features = {
        "entry_style_selected": style,
        "current_price": 101,
        "close": 101,
        "ma20": 95,
        "ma50": 90,
        "atr_pct": 0.04,
        "rs_percentile": ratio,
        "momentum_score": 75,
        "breakout_score": 75,
        "vcp_score": 75,
        "score_final": 75,
        "pivot": 100,
        "volume_missing": False,
    }
    ok, reasons, _meta = engine._evaluate_final30_entry_setup("036930", features, "KOSDAQ")
    assert ok is expected_pass, reasons
    if not expected_pass:
        assert "rs_too_low" in reasons
