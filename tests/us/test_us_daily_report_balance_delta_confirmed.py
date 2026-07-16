from pathlib import Path


def test_balance_delta_confirmed_fields_are_reported():
    text = Path('trader/us/runner/daily_report_runner.py').read_text()
    assert 'balance_delta_confirmed' in text
    assert 'balance_confirmed_count' in text
    assert 'orders_balance_confirmed_total' in text
