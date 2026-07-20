from pathlib import Path


def test_preflight_declares_canonical_path_and_override_policy():
    text = Path("scripts/wsl/deploy-preflight.sh").read_text()
    assert "/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored" in text
    for marker in ("PATH_MISMATCH", "STALE_CODE", "DIRTY_CODE", "DIRTY_GENERATED", "ALLOW_STALE_CODE", "ALLOW_DIRTY_CODE"):
        assert marker in text
