import json
import os
import shutil
import subprocess
import tarfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).parents[1]


def _copy_calendar_runtime(root: Path) -> None:
    (root / "scripts/wsl").mkdir(parents=True, exist_ok=True)
    for name in ("check-nullim-trading-day.py", "resolve-nullim-python.sh"):
        shutil.copy2(ROOT / "scripts/wsl" / name, root / "scripts/wsl" / name)
    (root / "trader").mkdir(exist_ok=True)
    shutil.copy2(ROOT / "trader/__init__.py", root / "trader/__init__.py")
    shutil.copy2(ROOT / "trader/time_utils.py", root / "trader/time_utils.py")
    (root / "config").mkdir(exist_ok=True)
    shutil.copy2(ROOT / "config/krx_holidays.json", root / "config/krx_holidays.json")


def fixture_us_repo(tmp_path: Path, trade_date: str, *, missing_purposes=()) -> Path:
    root = tmp_path / "repo"
    (root / "scripts/wsl").mkdir(parents=True)
    (root / "scripts/notify").mkdir(parents=True)
    for name in ("send-market-log-mail.sh", "send-us-log-mail.sh"):
        shutil.copy2(ROOT / "scripts/wsl" / name, root / "scripts/wsl" / name)
    shutil.copy2(ROOT / "scripts/notify/send_mail_attachment.py", root / "scripts/notify/send_mail_attachment.py")
    _copy_calendar_runtime(root)

    purposes = (
        "prep-prewarm-edt", "prep-prewarm-est", "prep", "prep-recovery",
        "am-preflight", "am", "afternoon", "close",
    )
    log_root = root / "runtime/logs/us" / trade_date
    for purpose in purposes:
        if purpose in set(missing_purposes):
            continue
        p = log_root / purpose
        p.mkdir(parents=True)
        (p / "attempt.log").write_text(f"purpose={purpose} APP_SECRET=do-not-leak\n", encoding="utf-8")
    log_root.mkdir(parents=True, exist_ok=True)
    (log_root / "session-manifest.json").write_text(
        json.dumps({"market": "US", "sessions": {}}), encoding="utf-8"
    )

    for item in (
        f"reports/us_daily/{trade_date}",
        f"reports/us_prep/{trade_date}",
        f"runtime/us/session_state/{trade_date}",
        f"runtime/us/watchlist/{trade_date}",
    ):
        p = root / item
        p.mkdir(parents=True)
        (p / "evidence.json").write_text('{"ok":true}', encoding="utf-8")
    health = root / f"reports/us_schedule_health/{trade_date}.json"
    health.parent.mkdir(parents=True)
    health.write_text("{}", encoding="utf-8")

    counter = root / "smtp-count"
    args_file = root / "smtp-args"
    wrapper = root / "scripts/wsl/with-venv.sh"
    wrapper.write_text(
        f'''#!/usr/bin/env bash
count=0
[[ -f "{counter}" ]] && count=$(cat "{counter}")
count=$((count+1))
echo "$count" > "{counter}"
printf '%s\\n' "$@" > "{args_file}"
echo '[MAIL][MESSAGE_ID] fixture-message'
exit 0
''',
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return root


def _init_fixture_git(root: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "config", "user.email", "fixture@example.test"], cwd=root, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=root, check=True)
    (root / "tracked-revision.txt").write_text("revision=1\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked-revision.txt"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture revision 1"], cwd=root, check=True)


def _advance_fixture_git_commit(root: Path) -> None:
    (root / "tracked-revision.txt").write_text("revision=2\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked-revision.txt"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture revision 2"], cwd=root, check=True)


def run_us_mail(root: Path, trade_date: str | None, *, kst_run_date: str):
    archive = root / "nullim-us-pr121.tar.gz"
    env = {
        **os.environ,
        "NULLIM_KST_RUN_DATE": kst_run_date,
        "NULLIM_LOG_MAIL_OUT": str(archive),
        "NULLIM_KEEP_MAIL_ARCHIVE": "1",
        "MAIL_TO": "ops@example.test",
        "NULLIM_TRADING_DAY_OVERRIDE": "open",
        "NULLIM_PYTHON_BIN": os.sys.executable,
        "NULLIM_SMTP_RETRY_SLEEP_1": "0",
        "NULLIM_SMTP_RETRY_SLEEP_2": "0",
        "NULLIM_MAIL_SNAPSHOT_LOCK_TIMEOUT_SEC": "1",
    }
    if trade_date is not None:
        env["US_TRADE_DATE"] = trade_date
    else:
        env.pop("US_TRADE_DATE", None)
    result = subprocess.run(
        ["bash", str(root / "scripts/wsl/send-us-log-mail.sh")],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
    )
    return result, archive


def test_us_missing_edt_prewarm_sends_degraded_mail(tmp_path):
    trade_date = "2026-09-10"
    run_date = "2026-09-11"
    root = fixture_us_repo(tmp_path, trade_date, missing_purposes={"prep-prewarm-edt"})

    result, archive = run_us_mail(root, trade_date, kst_run_date=run_date)

    assert result.returncode == 0, result.stderr + result.stdout
    assert (root / "smtp-count").read_text().strip() == "1"
    assert "status=DEGRADED" in result.stdout
    assert "REQUIRED_LOG_MISSING_prep-prewarm-edt" in result.stderr
    marker = json.loads((root / f"runtime/health/us-mail-{run_date}.json").read_text())
    assert marker["status"] == "DEGRADED"
    assert marker["mail_sent"] is True
    assert marker["mail_delivered"] is True
    assert marker["mail_complete"] is False
    assert marker["required_missing_count"] == 1
    assert marker["missing_evidence"] == [{
        "code": "REQUIRED_LOG_MISSING_prep-prewarm-edt",
        "path": f"runtime/logs/us/{trade_date}/prep-prewarm-edt",
    }]
    assert "[NULLIM][US][DEGRADED]" in (root / "smtp-args").read_text()

    with tarfile.open(archive, "r:gz") as tf:
        names = tf.getnames()
        manifest = json.load(tf.extractfile("./NULLIM_LOG_ARCHIVE_MANIFEST.json"))
        evidence = json.load(tf.extractfile("./mail_evidence_report.json"))
    assert manifest["evidence_status"] == "DEGRADED"
    assert manifest["required_missing_count"] == 1
    assert evidence["status"] == "DEGRADED"
    assert evidence["expected"] == 8
    assert evidence["present"] == 7
    assert evidence["missing"] == ["prep-prewarm-edt"]
    assert not any("prep-prewarm-edt/" in name for name in names)
    assert any("prep-prewarm-est/" in name for name in names)


def test_us_missing_core_am_log_still_delivers_degraded_mail(tmp_path):
    trade_date = "2026-09-10"
    run_date = "2026-09-11"
    root = fixture_us_repo(tmp_path, trade_date, missing_purposes={"am"})

    result, _ = run_us_mail(root, trade_date, kst_run_date=run_date)

    assert result.returncode == 0, result.stderr + result.stdout
    marker = json.loads((root / f"runtime/health/us-mail-{run_date}.json").read_text())
    assert marker["status"] == "DEGRADED"
    assert marker["mail_sent"] is True
    assert any(item["code"] == "REQUIRED_LOG_MISSING_am" for item in marker["missing_evidence"])


def test_degraded_evidence_digest_is_idempotent(tmp_path):
    trade_date = "2026-09-10"
    run_date = "2026-09-11"
    root = fixture_us_repo(tmp_path, trade_date, missing_purposes={"prep-prewarm-edt"})

    first, _ = run_us_mail(root, trade_date, kst_run_date=run_date)
    second, _ = run_us_mail(root, trade_date, kst_run_date=run_date)

    assert first.returncode == 0, first.stderr + first.stdout
    assert second.returncode == 0, second.stderr + second.stdout
    assert "IDEMPOTENT_SKIP" in second.stdout
    assert (root / "smtp-count").read_text().strip() == "1"


def test_evidence_digest_ignores_repo_commit_changes(tmp_path):
    trade_date = "2026-09-10"
    run_date = "2026-09-11"
    root = fixture_us_repo(tmp_path, trade_date, missing_purposes={"prep-prewarm-edt"})
    _init_fixture_git(root)

    first, _ = run_us_mail(root, trade_date, kst_run_date=run_date)
    first_marker = json.loads((root / f"runtime/health/us-mail-{run_date}.json").read_text())
    _advance_fixture_git_commit(root)
    second, _ = run_us_mail(root, trade_date, kst_run_date=run_date)

    assert first.returncode == 0, first.stderr + first.stdout
    assert second.returncode == 0, second.stderr + second.stdout
    assert "IDEMPOTENT_SKIP" in second.stdout
    assert (root / "smtp-count").read_text().strip() == "1"
    second_marker = json.loads((root / f"runtime/health/us-mail-{run_date}.json").read_text())
    assert second_marker["source_evidence_sha256"] == first_marker["source_evidence_sha256"]


def test_same_trade_evidence_is_idempotent_across_kst_run_dates(tmp_path):
    trade_date = "2026-09-10"
    first_run_date = "2026-09-11"
    second_run_date = "2026-09-12"
    root = fixture_us_repo(tmp_path, trade_date, missing_purposes={"prep-prewarm-edt"})

    first, _ = run_us_mail(root, trade_date, kst_run_date=first_run_date)
    second, _ = run_us_mail(root, trade_date, kst_run_date=second_run_date)

    assert first.returncode == 0, first.stderr + first.stdout
    assert second.returncode == 0, second.stderr + second.stdout
    assert "IDEMPOTENT_SKIP" in second.stdout
    assert (root / "smtp-count").read_text().strip() == "1"
    assert not (root / f"runtime/health/us-mail-{second_run_date}.json").exists()


def test_newly_arrived_missing_log_changes_digest_and_allows_complete_resend(tmp_path):
    trade_date = "2026-09-10"
    run_date = "2026-09-11"
    root = fixture_us_repo(tmp_path, trade_date, missing_purposes={"prep-prewarm-edt"})

    first, _ = run_us_mail(root, trade_date, kst_run_date=run_date)
    first_marker = json.loads((root / f"runtime/health/us-mail-{run_date}.json").read_text())
    restored = root / f"runtime/logs/us/{trade_date}/prep-prewarm-edt"
    restored.mkdir(parents=True)
    (restored / "attempt.log").write_text("purpose=prep-prewarm-edt\n", encoding="utf-8")
    second, _ = run_us_mail(root, trade_date, kst_run_date=run_date)

    assert first.returncode == 0, first.stderr + first.stdout
    assert first_marker["status"] == "DEGRADED"
    assert second.returncode == 0, second.stderr + second.stdout
    assert "IDEMPOTENT_SKIP" not in second.stdout
    assert (root / "smtp-count").read_text().strip() == "2"
    second_marker = json.loads((root / f"runtime/health/us-mail-{run_date}.json").read_text())
    assert second_marker["status"] == "OK"
    assert second_marker["required_missing_count"] == 0
    assert second_marker["source_evidence_sha256"] != first_marker["source_evidence_sha256"]


def test_us_delayed_mail_uses_latest_completed_partition(tmp_path):
    reference = datetime.now(ZoneInfo("America/New_York")).date()
    completed = (reference - timedelta(days=1)).isoformat()
    run_date = datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()
    root = fixture_us_repo(tmp_path, completed)

    result, _ = run_us_mail(root, None, kst_run_date=run_date)

    assert result.returncode == 0, result.stderr + result.stdout
    assert "[LOG_MAIL][TRADE_DATE_RECOVERY]" in result.stdout
    marker = json.loads((root / f"runtime/health/us-mail-{run_date}.json").read_text())
    assert marker["trade_date"] == completed


def test_health_distinguishes_degraded_delivery_from_mail_failure(tmp_path):
    trade_date = "2026-05-26"
    root = tmp_path / "health-repo"
    scripts = root / "scripts/wsl"
    scripts.mkdir(parents=True)
    shutil.copy2(ROOT / "scripts/wsl/check-nullim-day-health.sh", scripts / "check-nullim-day-health.sh")
    _copy_calendar_runtime(root)
    verify = scripts / "verify-no-nullim-auto-scheduler.sh"
    verify.write_text("#!/usr/bin/env bash\necho '[SCHEDULER_POLICY][WSL][OK] forbidden_sources=0'\n", encoding="utf-8")
    verify.chmod(0o755)

    log = root / f"runtime/logs/kr/{trade_date}/am/run.log"
    log.parent.mkdir(parents=True)
    log.write_text("[TICK]\n[TICK]\nsession_end\n", encoding="utf-8")

    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(["git", "config", "user.email", "fixture@example.test"], cwd=root, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=root, check=True)
    (root / "tracked").write_text("x", encoding="utf-8")
    subprocess.run(["git", "add", "tracked"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=root, check=True)
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()

    health = root / "runtime/health"
    health.mkdir(parents=True)
    (health / "windows-scheduler-install.json").write_text(json.dumps({
        "status": "OK",
        "scheduler_owner": "WINDOWS_TASK_SCHEDULER",
        "installed_commit_sha": sha,
    }), encoding="utf-8")
    (health / f"kr-mail-{trade_date}.json").write_text(json.dumps({
        "status": "DEGRADED",
        "mail_sent": True,
        "mail_delivered": True,
        "mail_complete": False,
        "market": "kr",
        "trade_date": trade_date,
        "required_missing_count": 1,
        "missing_evidence": [{"code": "REQUIRED_LOG_MISSING_close", "path": "runtime/logs/kr/x/close"}],
        "archive_sha256": "x",
    }), encoding="utf-8")

    env = {
        **os.environ,
        "NULLIM_TRADING_DAY_OVERRIDE": "open",
        "NULLIM_PYTHON_BIN": os.sys.executable,
    }
    result = subprocess.run(
        ["bash", str(scripts / "check-nullim-day-health.sh"), "kr", trade_date],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1
    report = json.loads((health / f"kr-{trade_date}.json").read_text())
    assert report["mail_delivered"] is True
    assert report["mail_complete"] is False
    assert report["mail_ok"] is False
    assert report["failure_reason"] == "DEGRADED_MAIL_EVIDENCE"
