from pathlib import Path

def test_close_only_passes_authoritative_direct_positions():
    s=Path('trader/us/runner/trade_close_runner.py').read_text()
    assert 'direct_positions = positions if final_positions_authoritative else None' in s
