from pathlib import Path


def test_generate_portfolio_pnl_report_does_not_import_removed_time_utils_path():
    path = Path("scripts/generate_portfolio_pnl_report.py")
    text = path.read_text(encoding="utf-8")
    assert "trader.utils.time_utils" not in text
