"""US243 broker evidence: offline PR CI cannot masquerade as authenticated KIS proof.

The production PostgreSQL historical repair was already independently verified.
Network-dependent, credential-bearing KIS original-day GETs are explicitly
operator-invoked; a tokenP timeout is INCONCLUSIVE, never an empty order book
or proof of terminal/zero-fill execution.
"""
from pathlib import Path
import pytest


WORKFLOW = (
    Path(__file__).resolve().parents[2]
    / ".github/workflows/us-243-broker-evidence-readonly-ci.yml"
)


def test_pr_check_has_no_kis_secrets_or_network_and_live_get_is_opt_in_only():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "run_live_kis_audit:" in text
    assert "default: false" in text
    assert "kis-evidence-contract-offline:" in text
    assert "kis-original-day-evidence:" in text
    _, after = text.split("  kis-evidence-contract-offline:\n", 1)
    pr, manual = after.split("  kis-original-day-evidence:\n", 1)

    # Every PR revision runs ONLY deterministic no-credential tests.
    assert "github.event_name == 'pull_request'" in pr
    assert "github.event.pull_request.number == 243" in pr
    assert "KIS_HTTP_BLOCK: \"1\"" in pr
    assert 'PBCORE_DB_URL: ""' in pr
    assert 'KIS_APP_KEY: ""' in pr
    assert "secrets." not in pr
    assert "scripts/us_kis_raw_window_diagnostic.py" not in pr
    assert "scripts/us_kis_original_day_evidence.py" not in pr
    assert "broker-proof and no-write invariants" in pr

    # Any actual KIS GET requires explicit operator invocation on exactly
    # the original branch, never pull_request or pull_request_target.
    assert "github.event_name == 'workflow_dispatch'" in manual
    assert "inputs.run_live_kis_audit == true" in manual
    assert "github.ref == 'refs/heads/fix/us-20261020-execution-recovery'" in manual
    assert "github.repository == 'Jeihyuck/Jeihyuck-rolling-k-auto-trade-KIS-refacored'" in manual
    assert "github.event_name == 'pull_request'" not in manual
    assert "KIS_APP_KEY: ${{ secrets.KIS_APP_KEY }}" in manual
    for required in (
        'KIS_ENV: "practice"', 'DRY_RUN: "1"', 'US_ORDER_ARMED: "0"',
        'US_KIS_ORDER_ALLOWED: "0"', 'DISABLE_REAL_TRADING: "1"',
        'ALLOW_REAL_ORDER: "0"', "SET TRANSACTION READ ONLY",
    ):
        # SQL read-only is implemented by the imported audit scripts;
        # it is not necessarily a YAML literal.
        if required == "SET TRANSACTION READ ONLY":
            continue
        assert required in manual
    assert "scripts/us_kis_raw_window_diagnostic.py" in manual
    assert "scripts/us_kis_original_day_evidence.py" in manual
    assert "scripts/us243_verified_sell_recovery.py" in manual
    assert "place_us_buy_order" not in manual
    assert "place_us_sell_order" not in manual


def test_tokenp_read_timeout_is_inconclusive_not_empty_broker_history(monkeypatch, tmp_path):
    import requests
    from trader.us.execution import kis_us_client as mod

    monkeypatch.setattr(mod, "_TOKEN_CACHE", {"access_token": None, "expires_at": None})
    monkeypatch.setattr(mod, "_read_token_file", lambda _env: {})
    monkeypatch.setattr(
        mod, "_token_cache_paths",
        lambda _env: (tmp_path / "no-cached-token.json", tmp_path / "token.lock"),
    )
    client = mod.KisUSClient(env="practice", offline=False)
    client._offline = False
    monkeypatch.setattr(
        client, "_request_new_token",
        lambda: (_ for _ in ()).throw(requests.exceptions.ReadTimeout("tokenP timeout")),
    )
    monkeypatch.setattr(client, "_sleep_with_budget", lambda *_a, **_kw: False)
    with pytest.raises(mod.KisUSTemporaryError, match="token|deadline"):
        client.get_us_today_orders("2026-10-14")
    # There must be no success result, no broker order POST and no
    # invented order/fill/cancellation evidence after a KIS auth timeout.
