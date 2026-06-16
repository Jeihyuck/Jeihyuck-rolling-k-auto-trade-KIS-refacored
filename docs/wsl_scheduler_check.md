# WSL Scheduler Audit

Use this PowerShell command on Windows to find tasks that may call WSL trading scripts:

```powershell
Get-ScheduledTask | Where-Object {$_.TaskName -match "kr|prep|trade|wsl"} | Select-Object TaskName,TaskPath,State
```

If any task invokes `scripts/wsl/run-kr-trader.sh`, disable it. PB1 scheduler must use `run-kr-prep.sh`, `run-kr-am.sh`, `run-kr-afternoon.sh`, or `run-kr-close.sh`.
