# Run from an Administrator PowerShell. Installs KR/US/mail/health WSL tasks as the single scheduler.
param([string]$Distro="Ubuntu-22.04", [string]$Repo="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored")
$ErrorActionPreference = "Stop"
$wsl = "$env:SystemRoot\System32\wsl.exe"
$base = "$Repo/scripts/wsl"
$defs = @(
  @{Name="PB1 KR Prep WSL"; Time="06:30"; Cmd="$base/run-kr-prep.sh"; Limit="PT2H"},
  @{Name="PB1 KR AM WSL"; Time="08:55"; Cmd="$base/run-kr-am.sh"; Limit="PT5H"},
  @{Name="PB1 KR Afternoon WSL"; Time="13:00"; Cmd="$base/run-kr-afternoon.sh"; Limit="PT3H"},
  @{Name="PB1 KR Close WSL"; Time="15:15"; Cmd="$base/run-kr-close.sh"; Limit="PT1H"},
  @{Name="PB1 KR Mail WSL"; Time="16:00"; Cmd="$base/send-market-log-mail.sh kr"; Limit="PT1H"},
  @{Name="PB1 KR Health WSL"; Time="16:10"; Cmd="$base/check-nullim-day-health.sh kr"; Limit="PT30M"},
  @{Name="PB1 US Prep WSL"; Time="21:30"; Cmd="$base/run-us-prep.sh"; Limit="PT2H"},
  @{Name="PB1 US Prep Guard WSL"; Time="22:20"; Cmd="bash $base/check-us-prep-before-am.sh"; Limit="PT30M"},
  @{Name="PB1 US Prep Recovery WSL"; Time="22:25"; Cmd="bash $base/run-us-prep-recovery.sh"; Limit="PT1H"},
  @{Name="PB1 US AM WSL"; Time="22:30"; Cmd="$base/run-us-am.sh"; Limit="PT5H"},
  @{Name="PB1 US Afternoon WSL"; Time="02:00"; Cmd="$base/run-us-afternoon.sh"; Limit="PT5H"},
  @{Name="PB1 US Close WSL"; Time="05:05"; Cmd="$base/run-us-close.sh"; Limit="PT1H"},
  @{Name="PB1 US Mail WSL"; Time="07:00"; Cmd="$base/send-market-log-mail.sh us"; Limit="PT1H"},
  @{Name="PB1 US Health WSL"; Time="07:10"; Cmd="$base/check-nullim-day-health.sh us"; Limit="PT30M"}
)
foreach ($d in $defs) {
  $action = New-ScheduledTaskAction -Execute $wsl -Argument "-d $Distro -- bash -lc 'cd $Repo && $($d.Cmd)'"
  $trigger = New-ScheduledTaskTrigger -Daily -At $d.Time
  $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -ExecutionTimeLimit ([System.Xml.XmlConvert]::ToTimeSpan($d.Limit))
  Register-ScheduledTask -TaskName $d.Name -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
  Write-Host "[SCHEDULER][OK] $($d.Name) time=$($d.Time) cmd=$($d.Cmd)"
}
Write-Host "[SCHEDULER][NOTE] WSL cron NULLIM block must be absent; run scripts/wsl/install-nullim-cron.sh inside WSL to remove/block it."
