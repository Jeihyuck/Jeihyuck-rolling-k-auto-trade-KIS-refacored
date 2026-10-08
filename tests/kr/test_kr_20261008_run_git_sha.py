"""2026-10-08: PB1 WSL runtime git revision was missing from all DB runs."""
import sqlalchemy as sa

from trader.db.repos import RunsRepo, _pb1_run_git_sha
from trader.db.schema import schema_for_engine


def _clear_revision_env(monkeypatch):
    for key in ("KR_RUN_REVISION", "NULLIM_RUN_REVISION", "GITHUB_SHA", "RUN_REVISION"):
        monkeypatch.delenv(key, raising=False)


def test_pb1_run_revision_prefers_explicit_and_environment(monkeypatch):
    _clear_revision_env(monkeypatch)
    explicit = "a" * 40
    env_rev = "b" * 40
    monkeypatch.setattr("trader.db.repos._pb1_checkout_sha", lambda: None)
    monkeypatch.setenv("KR_RUN_REVISION", env_rev)
    assert _pb1_run_git_sha(explicit, strategy="pb1_pullback_close") == explicit
    assert _pb1_run_git_sha(None, strategy="pb1_pullback_close") == env_rev
    assert _pb1_run_git_sha(None, strategy="us_final30") is None


def test_pb1_run_revision_reads_local_checkout_only_when_env_absent(monkeypatch):
    _clear_revision_env(monkeypatch)
    monkeypatch.setattr("trader.db.repos._pb1_checkout_sha", lambda: "c" * 40)
    monkeypatch.setenv("KR_RUN_REVISION", "b" * 40)
    assert _pb1_run_git_sha(None, strategy="pb1_pullback_close") == "c" * 40


def test_start_run_persists_revision_not_just_session_log(monkeypatch):
    _clear_revision_env(monkeypatch)
    monkeypatch.setattr("trader.db.repos._pb1_checkout_sha", lambda: None)
    monkeypatch.setenv("KR_RUN_REVISION", "d" * 40)
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    run_id = RunsRepo(engine).start_run(
        env="practice", strategy="pb1_pullback_close",
        run_window="morning", phase="entry", event_name="trade_am",
        dry_run=True, git_sha=None, workflow=None,
        workflow_run_id=None, workflow_attempt=None, config_json={},
    )
    with engine.connect() as conn:
        result = conn.execute(
            sa.select(schema.runs.c.git_sha).where(schema.runs.c.run_id == run_id)
        ).scalar_one()
    assert result == "d" * 40


def test_early_pb1_run_upsert_stamps_sha_even_when_no_later_tick(monkeypatch):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from trader import pb1_runner

    captured = {}

    class FakeRunsRepo:
        def __init__(self, engine):
            captured["engine"] = engine

        def upsert_run(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(pb1_runner, "RunsRepo", FakeRunsRepo)
    monkeypatch.setattr("trader.db.repos._pb1_run_git_sha", lambda *_a, **_kw: "e" * 40)
    monkeypatch.setenv("TRADER_RUN_ID", "early-run-id")
    ctx = SimpleNamespace(
        env="practice", strategy="pb1_pullback_close", gh_run_number=None,
        started_at=datetime.now(timezone.utc), git_sha=None,
    )
    repo, run_id = pb1_runner._register_kr_run_row(engine=object(), ctx=ctx)
    assert isinstance(repo, FakeRunsRepo)
    assert run_id == "early-run-id"
    assert captured["git_sha"] == "e" * 40


def test_early_pb1_run_with_legacy_default_strategy_still_stamps_real_revision(monkeypatch):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from trader import pb1_runner

    recorded = {}

    class FakeRunsRepo:
        def __init__(self, engine):
            pass

        def upsert_run(self, **kwargs):
            recorded.update(kwargs)

    monkeypatch.setattr(pb1_runner, "RunsRepo", FakeRunsRepo)
    monkeypatch.setattr("trader.db.repos._pb1_run_git_sha", lambda sha, *, strategy: (
        "f" * 40 if strategy == "pb1_pullback_close" else None
    ))
    monkeypatch.setenv("TRADER_RUN_ID", "legacy-ctx-early-run")
    ctx = SimpleNamespace(
        env="practice", strategy="best_k_meta",
        gh_run_number=None, started_at=datetime.now(timezone.utc), git_sha=None,
    )
    pb1_runner._register_kr_run_row(engine=object(), ctx=ctx)
    assert recorded["git_sha"] == "f" * 40
