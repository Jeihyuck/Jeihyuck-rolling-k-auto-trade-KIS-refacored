from pathlib import Path


def test_kis_final_balance_is_unconditional_canonical_source():
    text = Path('trader/us/runner/daily_report_runner.py').read_text()
    assert 'if final_balance_positions:' in text
    assert 'len(final_balance_positions) > len(positions)' not in text
    assert 'report["canonical_position_source"] = "kis_final_balance"' in text
