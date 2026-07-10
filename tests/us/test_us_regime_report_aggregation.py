import json

from trader.us.runner.trade_session_runner import _aggregate_regime_block_reporting


def test_session_report_aggregates_regime_block_reasons():
    out = _aggregate_regime_block_reporting([
        {
            "market_regime": "DEFENSIVE",
            "capital_scale": 0.25,
            "sector_cap_enforced": True,
            "blocked_entry_reason_counts": {"defensive_only_entry_block": 2},
        },
        {
            "market_regime": "DEFENSIVE",
            "capital_scale": 0.25,
            "sector_cap_enforced": True,
            "blocked_entry_reason_counts": {"defensive_only_entry_block": 3, "allow_ai_tech_buy_false": 1},
        },
    ])
    assert out["market_regime"] == "DEFENSIVE"
    assert out["capital_scale"] == 0.25
    assert out["sector_cap_enforced"] is True
    assert out["blocked_entry_reason_counts"] == {
        "defensive_only_entry_block": 5,
        "allow_ai_tech_buy_false": 1,
    }


def test_session_report_keeps_entry_block_reason_even_when_status_ok():
    out = _aggregate_regime_block_reporting([
        {
            "market_regime": "RISK_OFF",
            "capital_scale": 0.0,
            "sector_cap_enforced": True,
            "entry_degraded_reason": "risk_off_entry_block",
            "blocked_entry_reason_counts": {"risk_off_entry_block": 1},
        }
    ])
    assert out["trade_block_reason"] == "risk_off_entry_block"
    assert out["blocked_entry_reason_counts"]["risk_off_entry_block"] == 1


def test_daily_report_reads_blocked_entry_reason_counts_from_session_summary(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    summary_dir = tmp_path / "reports/us_daily/2026-07-09"
    summary_dir.mkdir(parents=True)
    (summary_dir / "am_summary.json").write_text(json.dumps({
        "market_regime": "DEFENSIVE",
        "capital_scale": 0.25,
        "sector_cap_enforced": True,
        "trade_block_reason": "defensive_only_entry_block",
        "blocked_entry_reason_counts": {"defensive_only_entry_block": 5, "allow_ai_tech_buy_false": 1},
    }), encoding="utf-8")

    from trader.us.runner.daily_report_runner import run_daily_report
    result = run_daily_report(env="practice", session="am", trade_date="2026-07-09", offline=True)
    report = result["report"]
    assert report["market_regime"] == "DEFENSIVE"
    assert report["capital_scale"] == 0.25
    assert report["sector_cap_enforced"] is True
    assert report["trade_block_reason"] == "defensive_only_entry_block"
    assert report["blocked_entry_reason_counts"] == {"defensive_only_entry_block": 5, "allow_ai_tech_buy_false": 1}
