$ErrorActionPreference = "Continue"
$expected = [ordered]@{
  "PB1 KR Prep WSL"      = "run-kr-prep.sh"
  "PB1 KR AM WSL"        = "run-kr-am.sh"
  "PB1 KR Afternoon WSL" = "run-kr-afternoon.sh"
  "PB1 KR Close WSL"     = "run-kr-close.sh"
  "PB1 US Prep WSL"      = "run-us-prep.sh"
  "PB1 US AM WSL"        = "run-us-am.sh"
  "PB1 US Afternoon WSL" = "run-us-afternoon.sh"
  "PB1 US Close WSL"     = "run-us-close.sh"
}
$krScripts = @("run-kr-prep.sh","run-kr-am.sh","run-kr-afternoon.sh","run-kr-close.sh")
$base = "/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored/scripts/wsl"
$failed = $false
foreach ($name in $expected.Keys) {
  $task = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
  if ($null -eq $task) { Write-Host "[SCHEDULER][FAIL] $name missing"; $failed = $true; continue }
  $actionText = ($task.Actions | Out-String)
  if ($name -like "PB1 KR*" -and $actionText -match "run-kr-trader\.sh") { Write-Host "[SCHEDULER][FAIL] $name still points to run-kr-trader.sh"; $failed = $true; continue }
  if ($actionText -match [regex]::Escape($expected[$name])) { Write-Host "[SCHEDULER][OK] $name -> $($expected[$name])" } else { Write-Host "[SCHEDULER][FAIL] $name expected $($expected[$name]) action=$actionText"; $failed = $true }
}
foreach ($script in $krScripts) {
  $check = wsl.exe -- bash -lc "test -x '$base/$script'"
  if ($LASTEXITCODE -ne 0) { Write-Host "[SCHEDULER][FAIL] KR script missing or not executable: $script"; $failed = $true }
}
if ($failed) { exit 1 }
exit 0
