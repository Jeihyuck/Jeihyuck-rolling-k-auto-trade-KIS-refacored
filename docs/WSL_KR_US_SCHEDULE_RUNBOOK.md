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

## PR #76 follow-up hardening

This follow-up preserves the Windows-only single-scheduler policy and its 16 canonical tasks. Canonical KR and US wrappers resolve the repository root from the real location of the wrapper file; `NULLIM_APP_DIR` is **not** an operational repository-selection override. A stale value inherited from a shell profile, `.env`, WSL, or Windows Task Scheduler is reported as `[NULLIM_PATH][STALE_ENV_IGNORED]`, then replaced with the resolved wrapper root. Windows task actions also unset it before launching the wrapper.

Order-capable tasks do not configure Windows automatic restart: their `RestartCount`/`RestartInterval` XML fields are omitted rather than set to zero. The scheduler verifier accepts a missing or zero `RestartCount` for those tasks and reads each action from its actual `Execute` and `Arguments` properties.

Every deploy-preflight invocation writes its context and outcome to `runtime/logs/deploy-preflight.log`, including the wrapper, market/session, inherited variable, resolved root, working directory, branch, commit, and failure reason. To safely verify stale-path handling without starting a session, run:

```bash
NULLIM_APP_DIR=/home/infiny/old-nullim-repository \
NULLIM_PREFLIGHT_ONLY=1 \
bash scripts/wsl/run-kr-am.sh

NULLIM_APP_DIR=/home/infiny/old-nullim-repository \
NULLIM_PREFLIGHT_ONLY=1 \
bash scripts/wsl/run-us-am.sh
```

After deployment, Windows administrator PowerShell must run the installer and verifier commands in the deployment section above. Linux CI cannot prove that Task Scheduler accepts the generated XML; confirm the final `[SCHEDULER_POLICY][OK]` output and inspect an order task's `RestartCount` (`$null` or `0` is correct).

Mail and Health actions render the wrapper script and `kr`/`us` market value as separate shell tokens. The Windows verifier checks this action contract and requires every canonical action to `unset NULLIM_APP_DIR`. Wrappers reassert their location-derived repository root immediately after loading `.env`, so `.env` cannot reintroduce either stale path variable. `deploy-preflight.log` must contain the wrapper's concrete market and session; `market=unknown` or `session=unknown` is not a valid production result.

## Verified market log delivery

Scheduled mail jobs invoke `send-kr-log-mail.sh` and `send-us-log-mail.sh`; they never pass a
market argument and can therefore never silently become an `all` archive. `all` is restricted
to manual diagnostics with `ALLOW_ALL_LOG_MAIL=1`. Each wrapper writes from bootstrap onward to
`runtime/logs/<market>/<KST-date>/<purpose>/<run-id>.log` and updates the dated session manifest.
US manifests additionally retain the New York trade date.

Mail packaging is fail-closed: dated session logs, health, reports and trading evidence are
copied into a private staging directory, redacted, manifested, archived, integrity checked and
size checked before SMTP. A successful checksum marker prevents Windows retry from sending the
same archive twice. Normal mail tasks do not use Task Scheduler automatic restart.

After every pull, run the administrator PowerShell installer and verifier shown above. A pull
does not update registered tasks. The installer records the installed commit in
`runtime/health/windows-scheduler-install.json`; drift is an operational failure. Windows Task
entry output is retained under `runtime/scheduler/windows/<KST-date>/`.

Raw session logs are retained for 90 days by operational policy; health and manifests should be
retained for at least one year. Mail staging and warnings are removed at the end of every run;
a successfully sent archive is removed immediately. Dry-run archives are retained for inspection.
