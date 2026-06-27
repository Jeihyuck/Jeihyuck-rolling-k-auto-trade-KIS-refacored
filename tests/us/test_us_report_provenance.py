def test_report_provenance_env_and_git_fallback(monkeypatch):
    from trader.us.runner.daily_report_runner import _report_provenance
    monkeypatch.setenv("GITHUB_REF_NAME", "dual-agent")
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    monkeypatch.setenv("GITHUB_WORKFLOW", "wf")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    p = _report_provenance("am")
    assert p["branch"] == "dual-agent"
    assert p["commit_sha"] == "abc123"
    assert p["workflow"] == "wf"
    assert p["run_id"] == "42"
