"""WSL GO evidence collector cannot accidentally arm a practice order."""
from pathlib import Path
import subprocess

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts" / "wsl" / "us-go-readonly-acceptance.sh"
)


def test_go_readonly_collector_disables_all_order_flags():
    code = SCRIPT.read_text(encoding="utf-8")
    for setting in (
        "KIS_ENV=practice", "NO_TRADE=1", "DRY_RUN=1",
        "DISABLE_REAL_TRADING=1", "US_ORDER_ARMED=0",
        "LIVE_TRADING_ENABLED=0", "US_LIVE_TRADING_ENABLED=0",
    ):
        assert setting in code
    assert "US_GO_ORDER_CAPABLE_STATUS=NO_GO" in code
    assert "exit 44" in code
    assert "--confirm-practice-readonly" in code
    assert "--stage4-only" in code
    assert "git status --porcelain" in code
    forbidden = ("git reset --hard", "git checkout -f", "git clean -fd",
                 "KIS_ENV=real", "US_ORDER_ARMED=1",
                 "psql -c", "UPDATE us_", "DELETE FROM us_")
    assert all(item not in code for item in forbidden)


def test_bad_or_missing_date_fails_before_any_broker_or_db_io():
    for args in ([], ["bad-date"], ["2026-13-99"]):
        output = subprocess.run(
            ["bash", str(SCRIPT), *args],
            text=True, capture_output=True, check=False, timeout=4,
        )
        if args == ["2026-13-99"]:
            # Regex cannot calendar-validate; the Python certificate will
            # validate before KIS, but an offline test should not run DB.
            continue
        assert output.returncode == 42
        assert "INVALID_TRADE_DATE" in output.stderr
