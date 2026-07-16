param([string]$Repo="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored")
$ErrorActionPreference = "Continue"
$expected = [ordered]@{
 "PB1 KR Prep WSL"=@("06:30","run-kr-prep.sh"); "PB1 KR AM WSL"=@("08:55","run-kr-am.sh"); "PB1 KR Afternoon WSL"=@("13:00","run-kr-afternoon.sh"); "PB1 KR Close WSL"=@("15:15","run-kr-close.sh"); "PB1 KR Mail WSL"=@("16:00","send-market-log-mail.sh kr"); "PB1 KR Health WSL"=@("16:10","check-nullim-day-health.sh kr");
 "PB1 US Prep Prewarm EDT WSL"=@("19:30","run-us-prep.sh"); "PB1 US Prep Prewarm EST WSL"=@("20:30","run-us-prep.sh"); "PB1 US Prep WSL"=@("21:30","run-us-prep.sh"); "PB1 US Prep Recovery WSL"=@("22:10","run-us-prep-recovery.sh"); "PB1 US AM Preflight WSL"=@("22:20","check-us-prep-before-am.sh"); "PB1 US AM WSL"=@("22:30","run-us-am.sh"); "PB1 US Afternoon WSL"=@("02:00","run-us-afternoon.sh"); "PB1 US Close WSL"=@("05:05","run-us-close.sh"); "PB1 US Mail WSL"=@("07:00","send-market-log-mail.sh us"); "PB1 US Health WSL"=@("07:10","check-nullim-day-health.sh us")
}
$failed=$false
foreach($name in $expected.Keys){
 $task=Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
 if($null -eq $task){Write-Host "[SCHEDULER][FAIL] $name missing"; $failed=$true; continue}
 $info=Get-ScheduledTaskInfo -TaskName $name -ErrorAction SilentlyContinue
 $action=($task.Actions | Out-String); $trig=($task.Triggers | Select-Object -First 1); $wantTime=$expected[$name][0]; $wantCmd=$expected[$name][1]
 $timeOk=($trig.StartBoundary -match "T$([regex]::Escape($wantTime)):")
 $settingsOk=($task.Settings.StartWhenAvailable -and $task.Settings.MultipleInstances -eq "IgnoreNew")
 $cmdOk=($action -match [regex]::Escape($wantCmd))
 if($cmdOk -and $timeOk -and $settingsOk){Write-Host "[SCHEDULER][OK] $name next=$($info.NextRunTime) last=$($info.LastTaskResult) time=$wantTime cmd=$wantCmd"} else {Write-Host "[SCHEDULER][FAIL] $name cmdOk=$cmdOk timeOk=$timeOk settingsOk=$settingsOk action=$action trigger=$($trig.StartBoundary)"; $failed=$true}
}
if($failed){exit 1}
