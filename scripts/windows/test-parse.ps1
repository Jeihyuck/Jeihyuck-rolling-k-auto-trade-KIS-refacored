param([string[]]$Paths = @("scripts/windows/update-nullim-scheduler.ps1", "scripts/windows/verify-scheduler.ps1", "scripts/windows/test-scheduler-trigger-contract.ps1"))
$errors = @()
foreach ($path in $Paths) {
  $tokens = $null; $parseErrors = $null
  [void][System.Management.Automation.Language.Parser]::ParseFile((Resolve-Path $path), [ref]$tokens, [ref]$parseErrors)
  if ($parseErrors.Count -gt 0) {
    $errors += $parseErrors | ForEach-Object {
      "{0}:{1}: {2}" -f $path, $_.Extent.StartLineNumber, $_.Message
    }
  }
}
if ($errors.Count -gt 0) { $errors | ForEach-Object { Write-Error $_ }; exit 1 }
Write-Host "[POWERSHELL_PARSE][OK] files=$($Paths.Count)"
