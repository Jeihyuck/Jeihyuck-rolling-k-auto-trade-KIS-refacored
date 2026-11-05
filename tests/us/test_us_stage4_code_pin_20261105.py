"""Stage4 worker must prove WSL pin == inherited SHA == Git HEAD."""
from pathlib import Path
import pytest

from trader.us.execution.stage4_code_integrity import assert_stage4_worker_code_pin


SHA = "a" * 40
OTHER = "b" * 40
DAY = "2026-11-05"


def _root(tmp_path: Path, pin: str = SHA) -> Path:
    folder = tmp_path / "runtime" / "code-pins"
    folder.mkdir(parents=True)
    (folder / f"US-{DAY}.sha").write_text(pin + "\n", encoding="ascii")
    return tmp_path


def _git(branch="dual-agent", sha=SHA):
    def read(*args):
        return branch if args == ("branch", "--show-current") else sha
    return read


def test_child_validates_exact_pin(tmp_path):
    assert assert_stage4_worker_code_pin(
        trade_date=DAY, expected_sha=SHA, repo_root=_root(tmp_path),
        git_read=_git(),
    ) == SHA


@pytest.mark.parametrize("expected,pin,branch,head,code", [
    ("", SHA, "dual-agent", SHA, "EXPECTED_MISSING"),
    (OTHER, SHA, "dual-agent", SHA, "FILE_MISMATCH"),
    (SHA, OTHER, "dual-agent", SHA, "FILE_MISMATCH"),
    (SHA, SHA, "feature", SHA, "BRANCH_MISMATCH"),
    (SHA, SHA, "dual-agent", OTHER, "HEAD_MISMATCH"),
])
def test_mixed_worker_sources_never_ready(tmp_path, expected, pin, branch, head, code):
    with pytest.raises(RuntimeError, match=code):
        assert_stage4_worker_code_pin(
            trade_date=DAY, expected_sha=expected,
            repo_root=_root(tmp_path, pin), git_read=_git(branch, head),
        )


def test_unpinned_worker_never_ready(tmp_path):
    with pytest.raises(RuntimeError, match="FILE_UNAVAILABLE"):
        assert_stage4_worker_code_pin(
            trade_date=DAY, expected_sha=SHA,
            repo_root=tmp_path, git_read=_git(),
        )
