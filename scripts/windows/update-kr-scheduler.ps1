# Backward-compatible wrapper. Prefer update-nullim-scheduler.ps1.
param([string]$Distro="Ubuntu-22.04", [string]$Repo="/home/infiny/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored")
& "$PSScriptRoot\update-nullim-scheduler.ps1" -Distro $Distro -Repo $Repo
exit $LASTEXITCODE
