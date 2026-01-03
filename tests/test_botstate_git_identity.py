import subprocess

from trader.botstate_sync import ensure_git_identity


def test_ensure_git_identity_sets_defaults(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init"], cwd=repo, check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", ""], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", ""], check=True)
    monkeypatch.setenv("GITHUB_ACTOR", "ci-actor")
    monkeypatch.delenv("BOTSTATE_GIT_NAME", raising=False)
    monkeypatch.delenv("BOTSTATE_GIT_EMAIL", raising=False)

    ensure_git_identity(repo)

    name = subprocess.run(["git", "-C", str(repo), "config", "user.name"], capture_output=True, text=True, check=True).stdout.strip()
    email = subprocess.run(["git", "-C", str(repo), "config", "user.email"], capture_output=True, text=True, check=True).stdout.strip()
    assert name == "ci-actor"
    assert email == "ci-actor@users.noreply.github.com"
