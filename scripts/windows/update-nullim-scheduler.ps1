# Windows Task Scheduler is the sole automatic scheduler for NULLIM KR/US sessions.
param([string]$Distro="Ubuntu-22.04", [string]$Repo="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored")
$ErrorActionPreference = "Stop"
$wsl = "$env:SystemRoot\System32\wsl.exe"
$base = "$Repo/scripts/wsl"
$defs = @(
 @{Name="PB1 KR Prep WSL";Time="06:30";Cmd="$base/run-kr-prep.sh";Limit="PT2H"}, @{Name="PB1 KR AM WSL";Time="08:55";Cmd="$base/run-kr-am.sh";Limit="PT5H"}, @{Name="PB1 KR Afternoon WSL";Time="13:00";Cmd="$base/run-kr-afternoon.sh";Limit="PT3H"}, @{Name="PB1 KR Close WSL";Time="15:15";Cmd="$base/run-kr-close.sh";Limit="PT1H"}, @{Name="PB1 KR Mail WSL";Time="16:00";Cmd="$base/send-market-log-mail.sh kr";Limit="PT1H"}, @{Name="PB1 KR Health WSL";Time="16:10";Cmd="$base/check-nullim-day-health.sh kr";Limit="PT30M"},
 @{Name="PB1 US Prep Prewarm EDT WSL";Time="19:30";Cmd="$base/run-us-prep.sh";Limit="PT2H"}, @{Name="PB1 US Prep Prewarm EST WSL";Time="20:30";Cmd="$base/run-us-prep.sh";Limit="PT2H"}, @{Name="PB1 US Prep WSL";Time="21:30";Cmd="$base/run-us-prep.sh";Limit="PT2H"}, @{Name="PB1 US Prep Recovery WSL";Time="22:10";Cmd="$base/run-us-prep-recovery.sh";Limit="PT1H"}, @{Name="PB1 US AM Preflight WSL";Time="22:20";Cmd="$base/check-us-prep-before-am.sh";Limit="PT30M"}, @{Name="PB1 US AM WSL";Time="22:30";Cmd="$base/run-us-am.sh";Limit="PT5H"}, @{Name="PB1 US Afternoon WSL";Time="02:00";Cmd="$base/run-us-afternoon.sh";Limit="PT5H"}, @{Name="PB1 US Close WSL";Time="05:05";Cmd="$base/run-us-close.sh";Limit="PT1H"}, @{Name="PB1 US Mail WSL";Time="07:00";Cmd="$base/send-market-log-mail.sh us";Limit="PT1H"}, @{Name="PB1 US Health WSL";Time="07:10";Cmd="$base/check-nullim-day-health.sh us";Limit="PT30M"}
)
# Cleanup must succeed before any task is changed, preventing a mixed-owner deployment.
& $wsl -d $Distro -- bash -lc "cd '$Repo' && bash scripts/wsl/install-nullim-cron.sh"
if ($LASTEXITCODE -ne 0) { throw "WSL NULLIM scheduler cleanup failed" }
$canonical = @($defs | ForEach-Object Name)
# Canonical identity is root TaskPath + task name; same name below another path is a duplicate.
$repoMarker = 'Jeihyuck-rolling-k-auto-trade-KIS-refacored'
$runnerMarker = 'run-kr-|run-us-|run_pb1_kr\.sh|send-market-log-mail\.sh|check-nullim-day-health\.sh|pb1_runner|trade_session_runner'
Get-ScheduledTask | ForEach-Object {
  $task = $_; $action = ($task.Actions | Out-String)
  # Preserve unrelated tasks even when they use a generically named runner.
  $isNullimTask = $action -match [regex]::Escape($repoMarker) -and $action -match $runnerMarker
  if (($canonical -notcontains $task.TaskName -or $task.TaskPath -ne "\") -and $isNullimTask) {
    Write-Host "[SCHEDULER][REMOVE_LEGACY] task=$($task.TaskName) action=$action"
    Unregister-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath -Confirm:$false
  }
}
$orderTasks = @("PB1 KR AM WSL","PB1 KR Afternoon WSL","PB1 KR Close WSL","PB1 US AM WSL","PB1 US Afternoon WSL","PB1 US Close WSL")
foreach ($d in $defs) {
  $restart = if ($orderTasks -contains $d.Name) { 0 } else { 3 }
  $action = New-ScheduledTaskAction -Execute $wsl -Argument "-d $Distro -- bash -lc 'cd $Repo && bash $($d.Cmd)'"
  $trigger = New-ScheduledTaskTrigger -Daily -At $d.Time
  $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -RestartCount $restart -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -ExecutionTimeLimit ([System.Xml.XmlConvert]::ToTimeSpan($d.Limit)) -WakeToRun
  $settings.DisallowStartIfOnBatteries=$false; $settings.StopIfGoingOnBatteries=$false; $settings.RunOnlyIfIdle=$false
  Register-ScheduledTask -TaskName $d.Name -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
  Write-Host "[SCHEDULER][OK] $($d.Name) time=$($d.Time) cmd=$($d.Cmd) restart_count=$restart"
}
& "$PSScriptRoot/verify-scheduler.ps1" -Repo $Repo -Distro $Distro
if ($LASTEXITCODE -ne 0) { throw "NULLIM scheduler verification failed" }
