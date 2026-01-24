# C:\MT5\scripts\pull_and_start.ps1
$ErrorActionPreference = "Stop"

$repo        = "C:\MT5"
$branch      = "main"
$runOrchFlag = "C:\MT5\RUN_ORCH"

# Actual start scripts (in /scripts)
$orchScript  = Join-Path $repo "scripts\start-orchestrator.ps1"
$mgrScript   = Join-Path $repo "scripts\start-manager.ps1"

function Log($msg) {
  $ts = Get-Date -Format o
  Write-Host "[$ts] $msg"
}

Log "Starting pull_and_start for $repo ($branch)"

# --- make sure git exists ---
$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
  throw "git not found in PATH. Install Git for Windows and restart the VPS."
}

if (!(Test-Path $repo)) {
  throw "Repo path not found: $repo"
}

Set-Location $repo

# --- ensure origin remote exists ---
$originUrl = (git remote get-url origin) 2>$null
if (-not $originUrl) {
  throw "No origin remote configured. Run: git remote add origin https://github.com/tboywixxy/buytrade.git"
}

Log "origin=$originUrl"

# --- force sync local with origin/main ---
Log "Fetching origin..."
git fetch origin --prune

Log "Checking out $branch..."
git checkout $branch

Log "Hard resetting to origin/$branch..."
git reset --hard ("origin/" + $branch)

# Optional: wipe anything not in git (DANGEROUS if you keep local state here)
# Log "Cleaning untracked files..."
# git clean -fd

Log "Repo sync complete. Starting services..."

# --- orchestrator (optional) ---
if (Test-Path $runOrchFlag) {
  if (!(Test-Path $orchScript)) {
    throw "RUN_ORCH flag exists, but orchestrator start script not found: $orchScript"
  }

  Log "RUN_ORCH flag found => starting orchestrator: $orchScript"
  Start-Process powershell.exe -WindowStyle Normal -ArgumentList @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-NoExit",
    "-File", $orchScript
  )
  Start-Sleep -Seconds 2
} else {
  Log "RUN_ORCH flag NOT found => skipping orchestrator (manager-only box)"
}

# --- manager (always) ---
if (!(Test-Path $mgrScript)) {
  throw "Manager start script not found: $mgrScript"
}

Log "Starting manager: $mgrScript"
Start-Process powershell.exe -WindowStyle Normal -ArgumentList @(
  "-NoProfile",
  "-ExecutionPolicy", "Bypass",
  "-NoExit",
  "-File", $mgrScript
)

Log "Done."
