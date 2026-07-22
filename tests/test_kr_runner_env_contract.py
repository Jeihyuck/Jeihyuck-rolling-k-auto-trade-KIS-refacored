from pathlib import Path


def test_kr_manual_dispatcher_delegates_to_canonical_wrappers():
    text = Path("run_pb1_kr.sh").read_text()
    assert "[MANUAL_DISPATCH][START]" in text
    assert "python -m trader.pb1_runner" not in text
    for wrapper in (
        "scripts/wsl/run-kr-prep.sh",
        "scripts/wsl/run-kr-am.sh",
        "scripts/wsl/run-kr-afternoon.sh",
        "scripts/wsl/run-kr-close.sh",
    ):
        assert wrapper in text
