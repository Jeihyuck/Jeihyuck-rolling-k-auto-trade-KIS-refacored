# Run from an Administrator PowerShell.
$base = "/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored/scripts/wsl"
$wsl = "C:\WINDOWS\System32\wsl.exe"
$distro = "Ubuntu-22.04"
$map = [ordered]@{
  "PB1 KR Prep WSL"      = "$base/run-kr-prep.sh"
  "PB1 KR AM WSL"        = "$base/run-kr-am.sh"
  "PB1 KR Afternoon WSL" = "$base/run-kr-afternoon.sh"
  "PB1 KR Close WSL"     = "$base/run-kr-close.sh"
}
foreach ($name in $map.Keys) {
  $script = $map[$name]
  $action = New-ScheduledTaskAction -Execute $wsl -Argument "-d $distro -- bash -lc '$script'"
  Set-ScheduledTask -TaskName $name -Action $action
  Write-Host "[OK] $name -> $script"
}
Write-Host ""
Write-Host "Verify with:"
Write-Host 'Get-ScheduledTask | Where-Object { $_.TaskName -match "PB1 KR|PB1 US" } | ForEach-Object { Write-Host "`n==== $($_.TaskName) ===="; $_.Actions }'
