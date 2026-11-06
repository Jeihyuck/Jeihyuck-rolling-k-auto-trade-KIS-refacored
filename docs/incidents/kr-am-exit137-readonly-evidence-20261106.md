# KR AM exit 137 — read-only evidence protocol (2026-11-06)

The observed process exit code **137** does **not** by itself prove an OOM kill, a timeout, or a task scheduler cancellation. Establish exact WSL/Python process start/end times and correlate those times with **kernel OOM / cgroup memory events** and **Windows Task Scheduler LastTaskResult** before assigning a root cause.

Use the WSL repo checkout to collect evidence without executing the trading engine or KIS calls:

```bash
bash scripts/wsl/collect-kr-exit137-evidence.sh runtime/diagnostics/exit137-20261106
```

The script writes only a local diagnostic text file, performs read-only system queries, and never modifies the scheduler, database, broker account or application configuration.

Evidence to cross-check:
- Windows Task Scheduler **LastRunTime / LastTaskResult** for KR AM, PREP, Afternoon and Close, plus Windows Event Viewer task history if available.
- WSL kernel messages: explicit `Out of memory`, `Killed process`, or cgroup memory kill; cgroup `memory.events` `oom_kill` counters.
- Process RSS, elapsed time and the exact point between the last structured runner stage and the exit.
- The relevant **per-tick timeout exit status**: distinguish an intentional Python timeout (124), external SIGKILL (137), broker rejection, and DB statement timeout.
- Check process death may also be caused by an administrator/parent task or external tooling; absent OOM evidence is not proof that OOM did not occur.

Acceptance criteria: an operator supplies timestamp-correlated kernel+task+runner evidence, and the PR investigation report classifies `EXPECTED`, `IMPLEMENTATION BUG`, `POLICY REVIEW`, or `UNVERIFIED` accordingly. Do not rewrite strategy behavior or silently raise the 180s KIS submit deadline to conceal infrastructure faults.

**Not completed by this PR:** actual WSL command execution, historical November 6 telemetry retrieval, or proof of the process-kill cause.
