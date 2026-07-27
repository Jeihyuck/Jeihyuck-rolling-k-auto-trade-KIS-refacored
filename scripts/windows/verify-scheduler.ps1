param(
  [string]$Repo="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored",
  [string]$Distro="Ubuntu-22.04",
  [switch]$SkipInstallMarkerCheck
)

function ConvertTo-NullimDayMask {
  param([object]$DaysOfWeek)
  if($null -eq $DaysOfWeek){return 0}
  if($DaysOfWeek -is [System.Enum] -or $DaysOfWeek -is [int] -or $DaysOfWeek -is [uint16] -or $DaysOfWeek -is [uint32]){return [int]$DaysOfWeek}
  $mask=0
  foreach($raw in @($DaysOfWeek)){
    foreach($day in ("$raw" -split '[, ]+' | Where-Object { $_ })){
      switch($day){
        'Sunday'{$mask=$mask -bor 1};'Monday'{$mask=$mask -bor 2};'Tuesday'{$mask=$mask -bor 4};'Wednesday'{$mask=$mask -bor 8};
        'Thursday'{$mask=$mask -bor 16};'Friday'{$mask=$mask -bor 32};'Saturday'{$mask=$mask -bor 64};
        default{if($day -match '^\d+$'){$mask=$mask -bor [int]$day}else{throw "Unknown DaysOfWeek value: $day"}}
      }
    }
  }
  return $mask
}

function New-NullimCheck {
  param(
    [bool]$Ok,
    [object]$Expected,
    [object]$Actual
  )
  return [pscustomobject]@{
    Ok = $Ok
    Expected = $Expected
    Actual = $Actual
  }
}

function Get-ActionText($Task) {
  return (@($Task.Actions) | ForEach-Object { "$($_.Execute) $($_.Arguments)" }) -join " "
}

$ErrorActionPreference="Continue"
$failed=$false
$wsl="$env:SystemRoot\System32\wsl.exe"
$base="$Repo/scripts/wsl"
$taskHasNotRunCode=267011 # 0x00041303 SCHED_S_TASK_HAS_NOT_RUN

$expected=[ordered]@{
  # Dedicated mail wrappers take no market argument; health requires an exact quoted argv token.
  "PB1 KR Prep WSL"=@("06:30","run-kr-prep.sh",$false,62);"PB1 KR AM WSL"=@("08:55","run-kr-am.sh",$true,62);"PB1 KR Afternoon WSL"=@("13:00","run-kr-afternoon.sh",$true,62);"PB1 KR Close WSL"=@("15:15","run-kr-close.sh",$true,62);"PB1 KR Mail WSL"=@("16:00","send-kr-log-mail.sh",$false,62);"PB1 KR Health WSL"=@("16:10","check-nullim-day-health.sh",$false,62);
  "PB1 US Prep Prewarm EDT WSL"=@("19:30","run-us-prep.sh",$false,62);"PB1 US Prep Prewarm EST WSL"=@("20:30","run-us-prep.sh",$false,62);"PB1 US Prep WSL"=@("21:30","run-us-prep.sh",$false,62);"PB1 US Prep Recovery WSL"=@("22:10","run-us-prep-recovery.sh",$false,62);"PB1 US AM Preflight WSL"=@("22:20","check-us-prep-before-am.sh",$false,62);"PB1 US AM WSL"=@("22:30","run-us-am.sh",$true,62);"PB1 US Afternoon WSL"=@("02:00","run-us-afternoon.sh",$true,124);"PB1 US Close WSL"=@("05:05","run-us-close.sh",$true,124);"PB1 US Mail WSL"=@("07:00","send-us-log-mail.sh",$false,124);"PB1 US Health WSL"=@("07:10","check-nullim-day-health.sh",$false,124)
}

foreach($name in $expected.Keys) {
  $info=Get-ScheduledTaskInfo -TaskName $name -TaskPath '\' -ErrorAction SilentlyContinue
  $task=Get-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction SilentlyContinue
  if($null -eq $task){
    Write-Host "[SCHEDULER][FAIL] task=$name field=Task expected=present actual=missing"
    $failed=$true
    continue
  }

  $a=Get-ActionText $task
  $trigs=@($task.Triggers)
  $e=$expected[$name]
  $actualDaysMask=if($trigs.Count -eq 1){ConvertTo-NullimDayMask $trigs[0].DaysOfWeek}else{0}
  $requiredArg=if($name -eq "PB1 KR Health WSL"){"kr"}elseif($name -eq "PB1 US Health WSL"){"us"}else{$null}
  $expectedArgFragment=if($null -eq $requiredArg){$null}else{"check-nullim-day-health.sh' '$requiredArg'"}
  $argumentOk=$null -eq $requiredArg -or $a.Contains($expectedArgFragment)
  $triggerStart=if($trigs.Count -eq 1){"$($trigs[0].StartBoundary)"}else{'none'}
  $triggerWeeks=if($trigs.Count -eq 1){$trigs[0].WeeksInterval}else{0}
  $restartOk=(!$e[2] -or $null -eq $task.Settings.RestartCount -or [int]$task.Settings.RestartCount -eq 0)

  $checks=[ordered]@{
    TaskPath=(New-NullimCheck -Ok ($task.TaskPath -eq '\') -Expected '\' -Actual $task.TaskPath)
    State=(New-NullimCheck -Ok ($task.State -ne 'Disabled') -Expected 'enabled' -Actual $task.State)
    RepoPath=(New-NullimCheck -Ok ($a -match [regex]::Escape($Repo)) -Expected $Repo -Actual $a)
    Distro=(New-NullimCheck -Ok ($a -match [regex]::Escape($Distro)) -Expected $Distro -Actual $a)
    Command=(New-NullimCheck -Ok ($a -match [regex]::Escape($e[1])) -Expected $e[1] -Actual $a)
    Argument=(New-NullimCheck -Ok $argumentOk -Expected $(if($null -eq $requiredArg){'none'}else{$requiredArg}) -Actual $a)
    UnsetNullimAppDir=(New-NullimCheck -Ok ($a -match 'unset[ ]+NULLIM_APP_DIR') -Expected 'unset NULLIM_APP_DIR' -Actual $a)
    BashLogin=(New-NullimCheck -Ok ($a -match 'bash -lc') -Expected 'bash -lc' -Actual $a)
    TriggerCount=(New-NullimCheck -Ok ($trigs.Count -eq 1) -Expected 1 -Actual $trigs.Count)
    DaysOfWeek=(New-NullimCheck -Ok ($actualDaysMask -eq [int]$e[3]) -Expected ([int]$e[3]) -Actual $actualDaysMask)
    WeeksInterval=(New-NullimCheck -Ok ($trigs.Count -eq 1 -and [int]$trigs[0].WeeksInterval -eq 1) -Expected 1 -Actual $triggerWeeks)
    StartBoundary=(New-NullimCheck -Ok ($trigs.Count -eq 1 -and $triggerStart -match "T$([regex]::Escape($e[0])):") -Expected $e[0] -Actual $triggerStart)
    StartWhenAvailable=(New-NullimCheck -Ok ([bool]$task.Settings.StartWhenAvailable) -Expected $true -Actual $task.Settings.StartWhenAvailable)
    MultipleInstances=(New-NullimCheck -Ok ($task.Settings.MultipleInstances -eq 'IgnoreNew') -Expected 'IgnoreNew' -Actual $task.Settings.MultipleInstances)
    WakeToRun=(New-NullimCheck -Ok ([bool]$task.Settings.WakeToRun) -Expected $true -Actual $task.Settings.WakeToRun)
    RunOnlyIfIdle=(New-NullimCheck -Ok (-not $task.Settings.RunOnlyIfIdle) -Expected $false -Actual $task.Settings.RunOnlyIfIdle)
    DisallowStartIfOnBatteries=(New-NullimCheck -Ok (-not $task.Settings.DisallowStartIfOnBatteries) -Expected $false -Actual $task.Settings.DisallowStartIfOnBatteries)
    StopIfGoingOnBatteries=(New-NullimCheck -Ok (-not $task.Settings.StopIfGoingOnBatteries) -Expected $false -Actual $task.Settings.StopIfGoingOnBatteries)
    RestartCount=(New-NullimCheck -Ok $restartOk -Expected 'null_or_0' -Actual $task.Settings.RestartCount)
  }

  $taskConfigOk=$true
  foreach($field in $checks.Keys){
    $c=$checks[$field]
    if(-not $c.Ok){
      $taskConfigOk=$false
      Write-Host "[SCHEDULER][FAIL] task=$name field=$field expected=$($c.Expected) actual=$($c.Actual) NextRunTime=$($info.NextRunTime) LastTaskResult=$($info.LastTaskResult)"
      $failed=$true
    }
  }

  if($null -eq $info){
    $runState='LAST_RUN_INFO_MISSING'
    $failed=$true
  }elseif([int64]$info.LastTaskResult -eq $taskHasNotRunCode -or $info.LastRunTime.Year -lt 2000){
    $runState='LAST_RUN_NOT_YET_EXECUTED'
  }elseif([int64]$info.LastTaskResult -eq 0){
    $runState='LAST_RUN_OK'
  }else{
    $runState='LAST_RUN_FAILED'
    $failed=$true
  }

  $configState=if($taskConfigOk){"CONFIG_OK"}else{"CONFIG_FAILED"}
  Write-Host "[SCHEDULER][$configState] task=$name LastRunTime=$($info.LastRunTime) LastTaskResult=$($info.LastTaskResult) NextRunTime=$($info.NextRunTime) run_state=$runState"
}

$canonical=@($expected.Keys)
$repoMarker='Jeihyuck-rolling-k-auto-trade-KIS-refacored'
$runnerMarker='run-kr-|run-us-|run_pb1_kr\.sh|send-(market|kr|us)-log-mail\.sh|check-nullim-day-health\.sh|pb1_runner|trade_session_runner'
$non=0
Get-ScheduledTask|ForEach-Object{
  $a=Get-ActionText $_
  $isNullimTask=$a -match [regex]::Escape($repoMarker) -and $a -match $runnerMarker
  if(($canonical -notcontains $_.TaskName -or $_.TaskPath -ne '\') -and $isNullimTask){
    Write-Host "[SCHEDULER][FAIL] reason=NON_CANONICAL_WINDOWS_TASK task=$($_.TaskName)"
    $non++
    $failed=$true
  }
}

if(-not $SkipInstallMarkerCheck){
  $installJson=& $wsl -d $Distro -- cat "$Repo/runtime/health/windows-scheduler-install.json"
  $installReadExit=$LASTEXITCODE
  $currentSha=(& $wsl -d $Distro -- git -C $Repo rev-parse HEAD).Trim()
  $currentReadExit=$LASTEXITCODE
  try{$installedSha=($installJson|ConvertFrom-Json).installed_commit_sha}catch{$installedSha="missing"}
  Write-Host "installed_commit_sha=$installedSha"
  Write-Host "current_commit_sha=$currentSha"
  if($installReadExit -ne 0 -or $currentReadExit -ne 0 -or $installedSha -ne $currentSha){
    $failed=$true
    Write-Host '[SCHEDULER][FAIL] reason=INSTALL_SHA_DRIFT'
  }
}else{
  Write-Host '[SCHEDULER][INFO] install_marker_check=SKIPPED_CONFIGURATION_PHASE'
}

& $wsl -d $Distro -- bash -lc "cd '$Repo' && bash scripts/wsl/verify-no-nullim-auto-scheduler.sh"
if($LASTEXITCODE -ne 0){
  $failed=$true
  Write-Host '[SCHEDULER_POLICY][FAIL] reason=FORBIDDEN_WSL_SCHEDULER'
}

if($failed){exit 1}
Write-Host '[SCHEDULER_POLICY][OK]'
Write-Host 'owner=WINDOWS_TASK_SCHEDULER'
Write-Host 'canonical_windows_tasks=16'
Write-Host "noncanonical_windows_tasks=$non"
Write-Host 'forbidden_wsl_sources=0'
