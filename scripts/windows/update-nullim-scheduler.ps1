# Windows Task Scheduler is the sole automatic scheduler for NULLIM KR/US sessions.
param([string]$Distro="Ubuntu-22.04", [string]$Repo="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored")
$ErrorActionPreference = "Stop"
function ConvertTo-BashSingleQuoted([string]$Value) {
  $singleQuote = [string][char]39
  $replacement = $singleQuote + '"' + $singleQuote + '"' + $singleQuote
  return $singleQuote + ($Value -replace $singleQuote, $replacement) + $singleQuote
}
function New-NullimTaskActionArguments([string]$Distro, [string]$Repo, [string]$Script, [string[]]$ScriptArgs, [string]$TaskName, [string]$InstallSha) {
  $repoQ=ConvertTo-BashSingleQuoted $Repo; $scriptQ=ConvertTo-BashSingleQuoted $Script
  $scriptArgsQ=@($ScriptArgs | ForEach-Object { ConvertTo-BashSingleQuoted $_ })
  $taskQ=ConvertTo-BashSingleQuoted $TaskName; $shaQ=ConvertTo-BashSingleQuoted $InstallSha
  $purpose = switch ($TaskName) { "PB1 US Prep Prewarm EDT WSL" { "prep-prewarm-edt" }; "PB1 US Prep Prewarm EST WSL" { "prep-prewarm-est" }; "PB1 US Prep WSL" { "prep" }; "PB1 US Prep Recovery WSL" { "prep-recovery" }; "PB1 US AM Preflight WSL" { "am-preflight" }; default { ([IO.Path]::GetFileNameWithoutExtension($Script) -replace '^run-','') } }
  $purposeQ=ConvertTo-BashSingleQuoted $purpose
  $entryQ=ConvertTo-BashSingleQuoted "$Repo/scripts/wsl/run-windows-scheduled-task.sh"
  $linuxCommand="unset NULLIM_APP_DIR; export NULLIM_SCHEDULER_OWNER=WINDOWS_TASK_SCHEDULER NULLIM_SCHEDULER_TASK_NAME=$taskQ NULLIM_SCHEDULER_INSTALL_SHA=$shaQ NULLIM_RUN_PURPOSE=$purposeQ; cd $repoQ && exec bash $entryQ $scriptQ"
  if($scriptArgsQ.Count -gt 0){$linuxCommand += " " + ($scriptArgsQ -join " ")}
  return "-d $Distro -- bash -lc `"$linuxCommand`""
}
$wsl = "$env:SystemRoot\System32\wsl.exe"; $base = "$Repo/scripts/wsl"
$krDays=@("Monday","Tuesday","Wednesday","Thursday","Friday")
$usEveningDays=@("Monday","Tuesday","Wednesday","Thursday","Friday")
$usMorningDays=@("Tuesday","Wednesday","Thursday","Friday","Saturday")
$installSha=(& $wsl -d $Distro -- bash -lc "git -C '$Repo' rev-parse HEAD").Trim(); if($LASTEXITCODE -ne 0){throw "Cannot resolve install SHA"}
$defs = @(
 @{Name="PB1 KR Prep WSL";Days=$krDays;Time="06:30";Script="$base/run-kr-prep.sh";Args=@();Limit="PT2H"}, @{Name="PB1 KR AM WSL";Days=$krDays;Time="08:55";Script="$base/run-kr-am.sh";Args=@();Limit="PT5H"}, @{Name="PB1 KR Afternoon WSL";Days=$krDays;Time="13:00";Script="$base/run-kr-afternoon.sh";Args=@();Limit="PT3H"}, @{Name="PB1 KR Close WSL";Days=$krDays;Time="15:15";Script="$base/run-kr-close.sh";Args=@();Limit="PT1H"}, @{Name="PB1 KR Mail WSL";Days=$krDays;Time="16:00";Script="$base/send-kr-log-mail.sh";Args=@();Limit="PT1H"}, @{Name="PB1 KR Health WSL";Days=$krDays;Time="16:10";Script="$base/check-nullim-day-health.sh";Args=@("kr");Limit="PT30M"},
 @{Name="PB1 US Prep Prewarm EDT WSL";Days=$usEveningDays;Time="19:30";Script="$base/run-us-prep.sh";Args=@();Limit="PT2H"}, @{Name="PB1 US Prep Prewarm EST WSL";Days=$usEveningDays;Time="20:30";Script="$base/run-us-prep.sh";Args=@();Limit="PT2H"}, @{Name="PB1 US Prep WSL";Days=$usEveningDays;Time="21:30";Script="$base/run-us-prep.sh";Args=@();Limit="PT2H"}, @{Name="PB1 US Prep Recovery WSL";Days=$usEveningDays;Time="22:10";Script="$base/run-us-prep-recovery.sh";Args=@();Limit="PT1H"}, @{Name="PB1 US AM Preflight WSL";Days=$usEveningDays;Time="22:20";Script="$base/check-us-prep-before-am.sh";Args=@();Limit="PT30M"}, @{Name="PB1 US AM WSL";Days=$usEveningDays;Time="22:30";Script="$base/run-us-am.sh";Args=@();Limit="PT5H"}, @{Name="PB1 US Afternoon WSL";Days=$usMorningDays;Time="02:00";Script="$base/run-us-afternoon.sh";Args=@();Limit="PT5H"}, @{Name="PB1 US Close WSL";Days=$usMorningDays;Time="05:05";Script="$base/run-us-close.sh";Args=@();Limit="PT1H"}, @{Name="PB1 US Mail WSL";Days=$usMorningDays;Time="07:00";Script="$base/send-us-log-mail.sh";Args=@();Limit="PT1H"}, @{Name="PB1 US Health WSL";Days=$usMorningDays;Time="07:10";Script="$base/check-nullim-day-health.sh";Args=@("us");Limit="PT30M"}
)
& $wsl -d $Distro -- bash -lc "cd '$Repo' && bash scripts/wsl/install-nullim-cron.sh"; if ($LASTEXITCODE -ne 0) { throw "WSL NULLIM scheduler cleanup failed" }
$canonical=@($defs|ForEach-Object Name);$repoMarker='Jeihyuck-rolling-k-auto-trade-KIS-refacored';$runnerMarker='run-kr-|run-us-|run_pb1_kr\.sh|send-(market|kr|us)-log-mail\.sh|check-nullim-day-health\.sh|pb1_runner|trade_session_runner'
Get-ScheduledTask|ForEach-Object{$action=(@($_.Actions)|ForEach-Object{"$($_.Execute) $($_.Arguments)"}) -join ' ';$isNullimTask=$action -match [regex]::Escape($repoMarker) -and $action -match $runnerMarker;if(($canonical -notcontains $_.TaskName -or $_.TaskPath -ne '\') -and $isNullimTask){Unregister-ScheduledTask -TaskName $_.TaskName -TaskPath $_.TaskPath -Confirm:$false}}
$orderTasks=@("PB1 KR Mail WSL","PB1 US Mail WSL","PB1 KR AM WSL","PB1 KR Afternoon WSL","PB1 KR Close WSL","PB1 US AM WSL","PB1 US Afternoon WSL","PB1 US Close WSL")
foreach($d in $defs){
  $restart=if($orderTasks -contains $d.Name){0}else{3}
  $actionArguments=New-NullimTaskActionArguments -Distro $Distro -Repo $Repo -Script $d.Script -ScriptArgs @($d.Args) -TaskName $d.Name -InstallSha $installSha
  $action=New-ScheduledTaskAction -Execute $wsl -Argument $actionArguments
  $trigger=New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $d.Days -At $d.Time
  if ($restart -gt 0){$settings=New-ScheduledTaskSettingsSet -StartWhenAvailable -RestartCount $restart -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -ExecutionTimeLimit ([System.Xml.XmlConvert]::ToTimeSpan($d.Limit)) -WakeToRun} else {$settings=New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit ([System.Xml.XmlConvert]::ToTimeSpan($d.Limit)) -WakeToRun}
  $settings.DisallowStartIfOnBatteries=$false;$settings.StopIfGoingOnBatteries=$false;$settings.RunOnlyIfIdle=$false
  Register-ScheduledTask -TaskName $d.Name -Action $action -Trigger $trigger -Settings $settings -Force|Out-Null
}
& "$PSScriptRoot/verify-scheduler.ps1" -Repo $Repo -Distro $Distro -SkipInstallMarkerCheck;if($LASTEXITCODE -ne 0){throw "NULLIM scheduler configuration verification failed"}
$markerPython = "import json,datetime,pathlib; p=pathlib.Path('runtime/health/windows-scheduler-install.json'); p.parent.mkdir(parents=True,exist_ok=True); now=datetime.datetime.now(datetime.timezone.utc).isoformat(); p.write_text(json.dumps({'status':'OK','scheduler_owner':'WINDOWS_TASK_SCHEDULER','installed_commit_sha':'$installSha','canonical_windows_tasks':16,'noncanonical_windows_tasks':0,'forbidden_wsl_sources':0,'installed_at':now,'verified_at':now},indent=2)+'\n')"
$markerRepoQ=ConvertTo-BashSingleQuoted $Repo
$markerPythonQ=ConvertTo-BashSingleQuoted $markerPython
& $wsl -d $Distro -- bash -lc "cd $markerRepoQ && python3 -c $markerPythonQ"; if($LASTEXITCODE -ne 0){throw "Scheduler install marker write failed"}
& "$PSScriptRoot/verify-scheduler.ps1" -Repo $Repo -Distro $Distro;if($LASTEXITCODE -ne 0){throw "NULLIM final scheduler verification failed"}
