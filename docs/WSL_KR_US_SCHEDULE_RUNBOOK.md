# KR/US Scheduler Runbook

## Single-owner production policy

**Windows Task Scheduler is the sole automatic owner for both KR and US trading.** WSL is the execution engine only: its canonical `scripts/wsl/run-*.sh` wrappers are called by the 16 Windows tasks. WSL user cron, system cron, and systemd timers must never run trading, prep, close, mail, or health jobs. GitHub Actions is restricted to CI, manual diagnostics, smoke tests, and reports; it is not an automatic live-order scheduler.

`run_pb1_kr.sh` is a manual compatibility dispatcher to the canonical KR wrappers. PostgreSQL advisory locks remain a final database guard, not permission to operate a second scheduler. Session file locks prevent overlap; a duplicate skip is a P0 health failure.

PR #60 and PR #67's single Windows scheduler policy is the final policy. PR #70's WSL cron installation regression must not be reintroduced.

## Deployment and recovery

After this PR is merged and pulled (do **not** run the pre-fix installer):

```bash
cd /home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
git fetch origin
git checkout dual-agent
git pull --ff-only origin dual-agent
mkdir -p runtime/scheduler-backups
crontab -l > "runtime/scheduler-backups/crontab-before-$(date +%Y%m%d-%H%M%S).txt" 2>/dev/null || true
bash scripts/wsl/install-nullim-cron.sh
bash scripts/wsl/verify-no-nullim-auto-scheduler.sh
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts/windows/update-nullim-scheduler.ps1 -Distro "Ubuntu-22.04" -Repo "/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
powershell -ExecutionPolicy Bypass -File scripts/windows/verify-scheduler.ps1 -Repo "/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored"
```

Success is `canonical_windows_tasks=16`, `noncanonical_windows_tasks=0`, and `forbidden_wsl_sources=0`.
