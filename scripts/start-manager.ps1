$ErrorActionPreference = "Continue"

$APP_DIR = "C:\MT5\app"
$PY      = "C:\MT5\worker_venv\Scripts\python.exe"
$HEALTH  = "http://127.0.0.1:9000/health"
$LOG     = "C:\MT5\logs\manager-uvicorn.log"

$env:MAX_ACCOUNTS_PER_VPS    = "50"
$env:IDLE_TTL_SEC            = "3600"
$env:REG_CLEAN_INTERVAL_SEC  = "300"

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

# If already our manager + healthy, do nothing
$pid9000 = Get-ListenerPid 9000
if ($pid9000) {
  $cmd = Get-CmdLine $pid9000
  if ($cmd -and $cmd -match "uvicorn" -and $cmd -match "manager_main:app" -and (Test-Health)) {
    "[OK] Manager already healthy (PID=$pid9000). Exiting." | Tee-Object -FilePath $LOG -Append
    exit 0
  }
}

"[WARN] Manager not healthy (or wrong process). Killing ports and starting..." | Tee-Object -FilePath $LOG -Append

# Kill listeners on 8000-8019 and 9000
$ports = @(8000..8019) + 9000
$pids = @()

try {
  $pids = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
    Where-Object { $ports -contains $_.LocalPort } |
    Select-Object -ExpandProperty OwningProcess -Unique
} catch {}

foreach ($p in $pids) {
  try { Stop-Process -Id $p -Force -ErrorAction SilentlyContinue } catch {}
}

Start-Sleep -Seconds 2

# Start manager (keep this PS alive; append logs)
Set-Location $APP_DIR
& $PY -m uvicorn manager_main:app --app-dir $APP_DIR --host 0.0.0.0 --port 9000 --workers 1 --log-level info *>> $LOG
