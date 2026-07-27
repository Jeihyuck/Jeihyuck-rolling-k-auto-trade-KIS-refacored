import json
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

ROOT = Path(__file__).parents[1]
DATE = "2099-01-02"


def fixture_repo(tmp_path: Path, market: str, *, smtp_mode: str = "dry") -> Path:
    root = tmp_path / "repo"
    (root / "scripts/wsl").mkdir(parents=True)
    (root / "scripts/notify").mkdir(parents=True)
    for name in ("send-market-log-mail.sh", f"send-{market}-log-mail.sh"):
        shutil.copy2(ROOT / "scripts/wsl" / name, root / "scripts/wsl" / name)
    shutil.copy2(ROOT / "scripts/notify/send_mail_attachment.py", root / "scripts/notify/send_mail_attachment.py")
    shutil.copy2(ROOT / "scripts/wsl/check-nullim-trading-day.py", root / "scripts/wsl/check-nullim-trading-day.py")
    purposes = ("prep", "am", "afternoon", "close") if market == "kr" else (
        "prep-prewarm-edt", "prep-prewarm-est", "prep", "prep-recovery", "am-preflight", "am", "afternoon", "close"
    )
    log_root = root / "runtime/logs" / market / DATE
    for purpose in purposes:
        p = log_root / purpose
        p.mkdir(parents=True)
        (p / "attempt-1.log").write_text(f"purpose={purpose} APP_SECRET=do-not-leak\n")
        (p / "attempt-2.log").write_text(f"purpose={purpose} retry=yes\n")
    (log_root / "session-manifest.json").write_text(json.dumps({"market": market.upper(), "sessions": {}}))
    if market == "kr":
        paths = (f"reports/kr_prep/{DATE}", f"runtime/kr/watchlist/{DATE}", f"runtime/kr/session/{DATE}", f"bot_state/trader_ledger/final30/practice/{DATE}")
    else:
        paths = (f"reports/us_daily/{DATE}", f"reports/us_prep/{DATE}", f"runtime/us/session_state/{DATE}", f"runtime/us/watchlist/{DATE}")
        health = root / f"reports/us_schedule_health/{DATE}.json"; health.parent.mkdir(parents=True); health.write_text("{}")
    for item in paths:
        p=root/item; p.mkdir(parents=True); (p/"evidence.json").write_text('{"ok":true}')
    counter=root/"smtp-count"
    if smtp_mode != "dry":
        wrapper=root/"scripts/wsl/with-venv.sh"
        wrapper.write_text(f'''#!/usr/bin/env bash
count=0; [[ -f "{counter}" ]] && count=$(cat "{counter}"); count=$((count+1)); echo "$count" > "{counter}"
if [[ "{smtp_mode}" == retry && "$count" -lt 3 ]]; then echo transient >&2; exit 1; fi
echo '[MAIL][MESSAGE_ID] fixture-message'; exit 0
''')
        wrapper.chmod(0o755)
    return root


def run_mail(root: Path, market: str, *, dry=True):
    archive=root/f"nullim-test-{market}-logs.tar.gz"
    env={**os.environ,"NULLIM_KST_RUN_DATE":DATE,"US_TRADE_DATE":DATE,"NULLIM_LOG_MAIL_OUT":str(archive),"NULLIM_KEEP_MAIL_ARCHIVE":"1","MAIL_TO":"ops@example.test","NULLIM_TRADING_DAY_OVERRIDE":"open","NULLIM_SMTP_RETRY_SLEEP_1":"0","NULLIM_SMTP_RETRY_SLEEP_2":"0"}
    if dry: env["NULLIM_MAIL_DRY_RUN"]="1"
    result=subprocess.run(["bash",str(root/f"scripts/wsl/send-{market}-log-mail.sh")],cwd=root,env=env,text=True,capture_output=True)
    return result,archive


def members(archive):
    with tarfile.open(archive,"r:gz") as tf: return tf.getnames(), tf.extractfile("./NULLIM_LOG_ARCHIVE_MANIFEST.json").read(), tf


def test_kr_end_to_end_uses_canonical_paths_and_dry_run_isolated(tmp_path):
    root=fixture_repo(tmp_path,"kr")
    result,archive=run_mail(root,"kr")
    assert result.returncode == 0, result.stderr+result.stdout
    assert "kr-logs" in archive.name and "all-logs" not in archive.name
    with tarfile.open(archive,"r:gz") as tf:
        names=tf.getnames(); manifest=json.load(tf.extractfile("./NULLIM_LOG_ARCHIVE_MANIFEST.json"))
        assert manifest["required_missing_count"] == 0
        assert sum(name.endswith(".log") for name in names) == 8
        assert any("reports/kr_prep" in name for name in names)
        assert any(f"bot_state/trader_ledger/final30/practice/{DATE}" in name for name in names)
        assert not any("bot_state/trader_ledger/final30/practice/2098-12-31" in name for name in names)
        assert not any("reports/kr/" in name or "runtime/trader_ledger" in name for name in names)
        assert b"do-not-leak" not in b"".join(tf.extractfile(n).read() for n in names if n.endswith(".log"))
    assert not (root/f"runtime/health/kr-mail-{DATE}.json").exists()
    dry=json.loads((root/f"runtime/health/kr-mail-dry-run-{DATE}.json").read_text())
    assert dry["status"] == "DRY_RUN" and dry["mail_sent"] is False


def test_us_cross_midnight_partition_collects_every_purpose(tmp_path):
    root=fixture_repo(tmp_path,"us")
    result,archive=run_mail(root,"us")
    assert result.returncode == 0, result.stderr+result.stdout
    with tarfile.open(archive,"r:gz") as tf:
        names=tf.getnames()
    for purpose in ("prep-prewarm-edt","prep-prewarm-est","prep","prep-recovery","am-preflight","am","afternoon","close"):
        assert any(f"runtime/logs/us/{DATE}/{purpose}/" in n for n in names)
    assert any(f"runtime/us/session_state/{DATE}" in n for n in names)
    assert not any(f"runtime/us/session/{DATE}" in n for n in names)


def test_stable_source_digest_skips_second_smtp_send(tmp_path):
    root=fixture_repo(tmp_path,"kr",smtp_mode="success")
    first,_=run_mail(root,"kr",dry=False); second,_=run_mail(root,"kr",dry=False)
    assert first.returncode == 0 and "attempt_count=1" in first.stdout
    assert second.returncode == 0 and "IDEMPOTENT_SKIP" in second.stdout
    assert (root/"smtp-count").read_text().strip() == "1"
    marker=json.loads((root/f"runtime/health/kr-mail-{DATE}.json").read_text())
    assert marker["source_evidence_sha256"] and marker["archive_sha256"]


def test_smtp_retries_twice_then_succeeds(tmp_path):
    root=fixture_repo(tmp_path,"kr",smtp_mode="retry")
    result,_=run_mail(root,"kr",dry=False)
    assert result.returncode == 0, result.stderr+result.stdout
    assert (root/"smtp-count").read_text().strip() == "3"
    marker=json.loads((root/f"runtime/health/kr-mail-{DATE}.json").read_text())
    assert marker["attempt_count"] == 3 and marker["status"] == "OK"

def test_session_manifest_preserves_attempts_and_effective_status(tmp_path):
    root=tmp_path/"session-repo"; scripts=root/"scripts/wsl"; scripts.mkdir(parents=True)
    shutil.copy2(ROOT/"scripts/wsl/init-session-log.sh",scripts/"init-session-log.sh")
    runner=scripts/"attempt.sh"
    runner.write_text('''#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/init-session-log.sh"
nullim_init_session_log KR am am "${BASH_SOURCE[0]}"
export NULLIM_SESSION_FINAL_STATUS="${FINAL_STATUS}"
export NULLIM_SESSION_FINAL_REASON="${FINAL_REASON}"
exit "${FINAL_RC}"
'''); runner.chmod(0o755)
    common={**os.environ,"NULLIM_KST_RUN_DATE":DATE}
    failed=subprocess.run(["bash",str(runner)],env={**common,"FINAL_STATUS":"FAILED","FINAL_REASON":"FIXTURE_FAILURE","FINAL_RC":"9"})
    ok=subprocess.run(["bash",str(runner)],env={**common,"FINAL_STATUS":"OK","FINAL_REASON":"SESSION_END","FINAL_RC":"0"})
    assert failed.returncode == 9 and ok.returncode == 0
    data=json.loads((root/f"runtime/logs/kr/{DATE}/session-manifest.json").read_text())
    attempts=data["sessions"]["am"]["attempts"]
    assert [a["status"] for a in attempts] == ["FAILED","OK"]
    assert data["sessions"]["am"]["effective_status"] == "OK"
    assert len(list((root/f"runtime/logs/kr/{DATE}/am").glob("*.log"))) == 2
    concurrent=[]
    for reason in ("CONCURRENT_ONE", "CONCURRENT_TWO"):
        concurrent.append(subprocess.Popen(["bash",str(runner)],env={**common,"FINAL_STATUS":"OK","FINAL_REASON":reason,"FINAL_RC":"0"}))
    assert all(proc.wait() == 0 for proc in concurrent)
    data=json.loads((root/f"runtime/logs/kr/{DATE}/session-manifest.json").read_text())
    attempts=data["sessions"]["am"]["attempts"]
    assert len(attempts) == 4
    assert {a["reason"] for a in attempts[-2:]} == {"CONCURRENT_ONE","CONCURRENT_TWO"}


def test_final_health_requires_real_mail_and_returns_matching_exit_code(tmp_path):
    root=tmp_path/"health-repo"; scripts=root/"scripts/wsl"; scripts.mkdir(parents=True)
    shutil.copy2(ROOT/"scripts/wsl/check-nullim-day-health.sh",scripts/"check-nullim-day-health.sh")
    shutil.copy2(ROOT/"scripts/wsl/check-nullim-trading-day.py",scripts/"check-nullim-trading-day.py")
    verify=scripts/"verify-no-nullim-auto-scheduler.sh"; verify.write_text("#!/usr/bin/env bash\necho '[SCHEDULER_POLICY][WSL][OK] forbidden_sources=0'\n"); verify.chmod(0o755)
    log=root/f"runtime/logs/kr/{DATE}/am/run.log"; log.parent.mkdir(parents=True); log.write_text("[TICK]\n[TICK]\nsession_end\n")
    subprocess.run(["git","init","-q"],cwd=root,check=True); subprocess.run(["git","config","user.email","fixture@example.test"],cwd=root,check=True); subprocess.run(["git","config","user.name","Fixture"],cwd=root,check=True)
    (root/"tracked").write_text("x"); subprocess.run(["git","add","tracked"],cwd=root,check=True); subprocess.run(["git","commit","-qm","fixture"],cwd=root,check=True)
    sha=subprocess.check_output(["git","rev-parse","HEAD"],cwd=root,text=True).strip(); health=root/"runtime/health"; health.mkdir(parents=True)
    (health/"windows-scheduler-install.json").write_text(json.dumps({"status":"OK","installed_commit_sha":sha}))
    health_env={**os.environ,"NULLIM_TRADING_DAY_OVERRIDE":"open"}
    missing=subprocess.run(["bash",str(scripts/"check-nullim-day-health.sh"),"kr",DATE],cwd=root,env=health_env)
    assert missing.returncode == 1
    (health/f"kr-mail-{DATE}.json").write_text(json.dumps({"status":"DRY_RUN","mail_sent":False,"market":"kr","trade_date":DATE,"required_missing_count":0,"archive_sha256":"x"}))
    dry=subprocess.run(["bash",str(scripts/"check-nullim-day-health.sh"),"kr",DATE],cwd=root,env=health_env)
    assert dry.returncode == 1
    (health/f"kr-mail-{DATE}.json").write_text(json.dumps({"status":"OK","mail_sent":True,"market":"kr","trade_date":DATE,"required_missing_count":0,"archive_sha256":"x"}))
    success=subprocess.run(["bash",str(scripts/"check-nullim-day-health.sh"),"kr",DATE],cwd=root,env=health_env)
    assert success.returncode == 0
