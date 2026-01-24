$ErrorActionPreference = "Stop"

$workdir = "C:\MT5\app"
$python  = "C:\MT5\worker_venv\Scripts\python.exe"

try {
  $p = (Get-NetTCPConnection -LocalPort 9000 -State Listen -ErrorAction SilentlyContinue).OwningProcess | Select-Object -First 1
  if ($p) { Stop-Process -Id $p -Force }
} catch {}

$cmd = @"
Set-Location -Path '$workdir'
& '$python' -m uvicorn manager_main:app --host 0.0.0.0 --port 9000 --log-level info
"@

Start-Process powershell.exe `
  -ArgumentList @("-NoExit","-NoProfile","-ExecutionPolicy","Bypass","-Command",$cmd) `
  -WindowStyle Normal
