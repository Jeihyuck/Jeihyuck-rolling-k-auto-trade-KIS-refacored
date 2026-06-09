from pathlib import Path


def test_us_claim_session_lock_writes_exception_output(monkeypatch, tmp_path, capsys):
    import scripts.us_claim_session_lock as mod

    def fail_claim(**_kwargs):
        raise RuntimeError("db unavailable")

    output = tmp_path / "lock.txt"
    monkeypatch.setattr(mod, "claim_us_session_lock", fail_claim)
    monkeypatch.setattr(
        "sys.argv",
        [
            "us_claim_session_lock.py",
            "--session",
            "am",
            "--trade-date",
            "2026-06-09",
            "--output",
            str(output),
        ],
    )

    assert mod.main() == 0
    text = output.read_text(encoding="utf-8")
    assert "claimed=0" in text
    assert "existing_status=session_lock_exception" in text
    assert "session_lock_exception=RuntimeError: db unavailable" in text
    captured = capsys.readouterr()
    assert "session_lock_exception=RuntimeError: db unavailable" in captured.out
    assert "::error::session_lock_exception RuntimeError: db unavailable" in captured.err
