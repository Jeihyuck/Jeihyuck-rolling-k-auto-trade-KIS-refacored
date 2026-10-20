"""GitHub-hosted KIS original-day audit must never inherit trading permissions."""
from pathlib import Path


def test_pr243_github_hosted_recovery_audit_is_separate_and_read_only():
    path = Path(__file__).resolve().parents[2] / ".github/workflows/us-agent.yml"
    definition = path.read_text(encoding="utf-8")
    assert "          - recovery-audit" in definition
    assert "  manual:\n    if: ${{ inputs.mode != 'recovery-audit' }}" in definition
    assert "  original_day_recovery_evidence:\n    if: ${{ inputs.mode == 'recovery-audit' }}" in definition

    audit = definition.split("  original_day_recovery_evidence:", 1)[1]
    assert "runs-on: ubuntu-latest" in audit
    assert "https://openapivts.koreainvestment.com:29443" in audit
    for flag in (
        'KIS_ENV: "practice"',
        'DRY_RUN: "1"',
        'US_ORDER_ARMED: "0"',
        'US_KIS_ORDER_ALLOWED: "0"',
        'ALLOW_REAL_ORDER: "0"',
        'US_LIVE_TRADING_ENABLED: "0"',
        'DISABLE_REAL_TRADING: "1"',
    ):
        assert flag in audit
    assert "scripts/us_kis_original_day_evidence.py" in audit
    assert "--require-all-terminal" in audit
    for forbidden in ("trader.us.runner.dispatcher", "trader.db.migrate",
                      "place_us_sell_order", "place_us_buy_order",
                      "workflow_run", "schedule:", "pull_request_target:"):
        assert forbidden not in audit
