$ErrorActionPreference = "Continue"

$APP_DIR = "C:\MT5\app"
# use orchestrator venv if you have it; otherwise worker venv
$PY = "C:\MT5\orchestrator_venv\Scripts\python.exe"
if (!(Test-Path $PY)) { $PY = "C:\MT5\worker_venv\Scripts\python.exe" }

$HEALTH  = "http://127.0.0.1:9100/health"
$LOG     = "C:\MT5\logs\orchestrator-uvicorn.log"

function Test-Health {
  try {
    $r = Invoke-WebRequest -UseBasicParsing -Uri $HEALTH -TimeoutSec 2
    return ($r.StatusCode -eq 200)
  } catch { return $false }
}

function Get-ListenerPid([int]$port) {
  try {
    return (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue |
      Select-Object -First 1 -ExpandProperty OwningProcess)
  } catch { return $null }
}

function Get-CmdLine([int]$pid) {
  try { return (Get-CimInstance Win32_Process -Filter "ProcessId=$pid" -ErrorAction SilentlyContinue).CommandLine }
  catch { return $null }
}

$pid9100 = Get-ListenerPid 9100
if ($pid9100) {
  $cmd = Get-CmdLine $pid9100
  if ($cmd -and $cmd -match "uvicorn" -and $cmd -match "orchestrator_service:app" -and (Test-Health)) {
    "[OK] Orchestrator already healthy (PID=$pid9100). Exiting." | Tee-Object -FilePath $LOG -Append
    exit 0
  }
}

"[WARN] Orchestrator not healthy (or wrong process). Killing 9100 and starting..." | Tee-Object -FilePath $LOG -Append

if ($pid9100) {
  try { Stop-Process -Id $pid9100 -Force -ErrorAction SilentlyContinue } catch {}
}

Start-Sleep -Seconds 2

Set-Location $APP_DIR
& $PY -m uvicorn orchestrator_service:app --app-dir $APP_DIR --host 0.0.0.0 --port 9100 --workers 1 --log-level info *>> $LOG
