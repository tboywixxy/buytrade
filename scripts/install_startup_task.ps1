# C:\MT5\scripts\install_startup_task.ps1
$ErrorActionPreference = "Stop"

$taskName = "MT5-Pull-And-Start"
$script   = "C:\MT5\scripts\pull_and_start.ps1"

if (!(Test-Path $script)) {
  throw "Missing: $script"
}

# Delete existing task if any
schtasks /Delete /TN $taskName /F 2>$null | Out-Null

# Create startup task (SYSTEM)
schtasks /Create /TN $taskName `
  /SC ONSTART `
  /RU "SYSTEM" `
  /RL HIGHEST `
  /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$script`""

Write-Host "[OK] Installed startup task: $taskName"
Write-Host "It will run: $script"
