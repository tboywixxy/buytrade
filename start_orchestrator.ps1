$ErrorActionPreference = "Stop"

$workdir = "C:\MT5\app"
$python  = "C:\MT5\worker_venv\Scripts\python.exe"

if ([Environment]::GetEnvironmentVariable("ORCH_ENABLED","Machine") -ne "1") { exit 0 }

try {
  $p = (Get-NetTCPConnection -LocalPort 9100 -State Listen -ErrorAction SilentlyContinue).OwningProcess | Select-Object -First 1
  if ($p) { Stop-Process -Id $p -Force }
} catch {}

$cmd = @"
Set-Location -Path '$workdir'
& '$python' -m uvicorn orchestrator_service:app --host 0.0.0.0 --port 9100 --log-level info
"@

Start-Process powershell.exe `
  -ArgumentList @("-NoExit","-NoProfile","-ExecutionPolicy","Bypass","-Command",$cmd) `
  -WindowStyle Normal
