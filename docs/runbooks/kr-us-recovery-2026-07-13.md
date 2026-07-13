# KR/US emergency recovery runbook (2026-07-13)

## Rollback baseline

The last known US-market recovery candidate is commit `9395f40e8524cc97f88e4bf7627705d72ab3f2ba` (`Finalize PR49 overlay provider fallbacks`). It is the closest repository point to the 2026-07-10 US afternoon log where the tick loop reached tick 25 and exited by graceful shutdown.

Do **not** force-push `dual-agent` as part of emergency triage. Create and keep a backup branch first.

```bash
git branch backup/before-kr-us-recovery-$(date +%Y%m%d-%H%M%S)
git fetch origin
git checkout dual-agent
# optional local-only rollback if operations chooses it:
git reset --hard 9395f40e8524cc97f88e4bf7627705d72ab3f2ba
```

## Post-rollback verification

```bash
scripts/wsl/run-us-prep.sh
python - <<'PY'
import json
from pathlib import Path
for path in [Path('reports/us_prep/latest_us_prep_summary.json'), Path('runtime/us/prep_status/latest/prep_status.json')]:
    if path.exists():
        data=json.loads(path.read_text(encoding='utf-8'))
        c=data.get('contract') or data
        print(path, 'status=', c.get('status'), 'trade_can_proceed=', c.get('trade_can_proceed'), 'entry_can_proceed=', c.get('entry_can_proceed'), 'exit_can_proceed=', c.get('exit_can_proceed'), 'close_can_proceed=', c.get('close_can_proceed'))
PY
scripts/wsl/run-us-am.sh
scripts/wsl/run-us-afternoon.sh
```

Success means US Prep status is `OK` or safe `OK_WITH_WARNINGS*`, `trade_can_proceed=1`, and US AM/afternoon enters `[US_TICK_LOOP][TICK` without `[US_PREP_GUARD][BLOCK]` for safe underfilled prep.

## Scheduler installation and verification on Windows/WSL

Windows Task Scheduler is the single scheduler for WSL. The WSL cron installer removes the NULLIM block and exits with `[CRON_INSTALL][BLOCK] reason=WSL_USES_WINDOWS_TASK_SCHEDULER` on WSL.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/windows/update-kr-scheduler.ps1
powershell -ExecutionPolicy Bypass -File scripts/windows/verify-scheduler.ps1
```

```bash
scripts/wsl/install-nullim-cron.sh
crontab -l | sed -n '/NULLIM_CRON_START/,/NULLIM_CRON_END/p'
```

The final command must print nothing.

## Acceptance log examples

KR AM/PM loop acceptance:

```text
[KR_AM][PB1 loop enabled] PB1_LOOP_ENABLED=1 PB1_RUN_LOOP=1 PB1_RUN_LOOP_MINUTES=235 PB1_LOOP_INTERVAL_SEC=60 PB1_AM_SESSION_END=12:55
[TICK] tick=1
[TICK] tick=2
[KR_AM][EXIT] ... reason=session_end
```

US degraded prep acceptance:

```text
[US_PREP][CONTRACT] status=OK_WITH_WARNINGS_CLUSTER_CAP trade_can_proceed=1 entry_can_proceed=0 exit_can_proceed=1 close_can_proceed=1 raw_final30_count=17 effective_final30_count=17
[US_PREP_GUARD][OK] workflow=us-trade-am ... final30=17 ... effective_max_new_positions=0
[US_TICK_LOOP][TICK] session=am tick=1
```

Mail and health acceptance:

```text
[LOG_MAIL][OK] market=kr ... marker=runtime/health/kr-mail-YYYY-MM-DD.json
NULLIM KR health YYYY-MM-DD
- mail_ok: True
```
