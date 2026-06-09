from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.pb1_engine import resolve_pb1_phase
from trader.strategies.pb1_pullback_close import evaluate_setup


def test_afternoon_late_start_resolves_pm_entry_before_close(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "afternoon")
    monkeypatch.setenv("PB1_PM_SESSION_END", "15:10")
    phase, reason, _window = resolve_pb1_phase(
        datetime(2026, 6, 9, 13, 41, tzinfo=ZoneInfo("Asia/Seoul")),
        True,
        "exit",
    )
    assert phase == "pm_entry"
    assert reason == "afternoon_entry_allowed"


def test_afternoon_close_window_resolves_close(monkeypatch):
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "afternoon")
    monkeypatch.setenv("PB1_PM_SESSION_END", "15:10")
    phase, reason, _window = resolve_pb1_phase(
        datetime(2026, 6, 9, 15, 12, tzinfo=ZoneInfo("Asia/Seoul")),
        True,
        "pm_entry",
    )
    assert phase == "close"
    assert reason == "close_window"


def test_evaluate_setup_uses_explicit_pb1_threshold_overrides():
    ok, reasons = evaluate_setup(
        {
            "close": 101.0,
            "ma20": 100.0,
            "ma50": 90.0,
            "pullback_pct": 5.0,
            "vol_contraction": 1.20,
            "volu_contraction": 1.20,
            "ma20_slope": 1.0,
            "volume_missing": False,
            "_pb1_vol_max": 1.25,
            "_pb1_volu_max": 1.25,
        },
        "KOSPI",
        require_volume=True,
        mode="relaxed",
        relax_ma_filter=True,
        relax_ma20_slope=True,
    )
    assert ok, reasons
    assert "vol_contraction_fail" not in reasons
    assert "volu_contraction_fail" not in reasons


def test_intraday_reclaim_equivalent_features_pass_close_below_ma20():
    last_close = 99.0
    ma20 = 100.0
    current_price = ma20 * 1.001
    features = {
        "last_close": last_close,
        "close": current_price,
        "current_price": current_price,
        "ma20": ma20,
        "ma50": 90.0,
        "pullback_pct": 5.0,
        "vol_contraction": 0.9,
        "volu_contraction": 0.9,
        "ma20_slope": 1.0,
        "volume_missing": False,
        "quality_flags": ["INTRADAY_RECLAIM_MA20"],
    }
    ok, reasons = evaluate_setup(
        features,
        "KOSPI",
        require_volume=True,
        mode="relaxed",
        relax_ma_filter=True,
        relax_ma20_slope=True,
    )
    assert ok, reasons
    assert "close_below_ma20" not in reasons
    assert "INTRADAY_RECLAIM_MA20" in features["quality_flags"]


def test_pm_entry_normalization_preserves_phase(monkeypatch):
    from trader import pb1_runner

    monkeypatch.setenv("PB1_SESSION_KIND", "afternoon")
    window_name, phase_name = pb1_runner._normalize_window_phase(
        raw_window="day",
        market_window="afternoon",
        phase="pm_entry",
    )
    assert window_name == "day"
    assert phase_name == "pm_entry"


def test_db_connect_args_include_postgres_timeouts(monkeypatch):
    from trader.db.engine import _connect_args_for_db_url

    monkeypatch.setenv("DB_LOCK_TIMEOUT_MS", "5000")
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "15000")
    monkeypatch.setenv("DB_IDLE_IN_TX_SESSION_TIMEOUT_MS", "15000")
    args = _connect_args_for_db_url("postgresql+psycopg://user:pass@example.com/db")
    options = args.get("options", "")
    assert "-c lock_timeout=5000" in options
    assert "-c statement_timeout=15000" in options
    assert "-c idle_in_transaction_session_timeout=15000" in options


def test_reject_summary_fallback_never_emits_none(caplog):
    from types import SimpleNamespace

    from trader.pb1_engine import PB1Engine

    fake = SimpleNamespace(
        reject_reason_counts={},
        reject_reason_samples={},
        total_candidates=2,
        ok_count=0,
        _reject_summary_candidates=[
            SimpleNamespace(code="005930", features={"setup_loose_reasons": ["close_below_ma20"]}, reasons=[]),
            SimpleNamespace(code="000660", features={"setup_strict_reasons": ["vol_contraction_fail"]}, reasons=[]),
        ],
    )
    with caplog.at_level("INFO"):
        PB1Engine._log_reason_summary(fake, "unit_test")
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "[ENTRY][REJECT_SUMMARY][FALLBACK]" in text
    assert "[RUN_SUMMARY][NO_BUY] reason=NO_FINAL_SETUPS top_blockers=none" not in text
    assert "CLOSE_BELOW_MA20" in text


def test_intraday_reclaim_missing_current_price_logs_skip(caplog):
    from types import SimpleNamespace

    from trader.pb1_engine import PB1Engine

    fake = SimpleNamespace(
        phase="pm_entry",
        price_allowed=False,
        _balance_price_map={},
        _to_float=PB1Engine._to_float,
    )
    with caplog.at_level("INFO"):
        result = PB1Engine._resolve_intraday_current_price_for_reclaim(
            fake,
            "005930",
            current_price=None,
            last_close=99.0,
            ma20_value=100.0,
        )
    assert result is None
    assert "[ENTRY][INTRADAY_RECLAIM][SKIP] code=005930 reason=current_price_missing" in "\n".join(
        record.getMessage() for record in caplog.records
    )
