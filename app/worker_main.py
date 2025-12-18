import os
import shutil
import time
import subprocess
import threading
from typing import Optional, Dict, Any, List, Tuple

from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

import MetaTrader5 as MT5
from fastapi import FastAPI, HTTPException, Body, UploadFile, File, Request
from fastapi.responses import JSONResponse

try:
    import psutil
except ImportError:
    psutil = None

app = FastAPI(title="MT5 Worker (portable + session-safe)", version="1.6.0")

# ===== CONFIG =====
INSTANCES_ROOT = os.environ.get("MT5_INSTANCES_ROOT", r"C:\MT5\instances")
BROKERS_DIR = os.environ.get("MT5_BROKERS_DIR", r"C:\MT5\brokers")
BASE_MT5_DIR = os.environ.get("MT5_BASE_DIR", r"C:\MT5\mt5-terminal")

WORKER_PORT = os.environ.get("MT5_WORKER_PORT", "").strip()
if not WORKER_PORT or WORKER_PORT == "0":
    raise RuntimeError("MT5_WORKER_PORT env is REQUIRED for worker. Manager must set it.")

TRASH_DIR = os.path.join(INSTANCES_ROOT, "_trash")
os.makedirs(INSTANCES_ROOT, exist_ok=True)
os.makedirs(BROKERS_DIR, exist_ok=True)
os.makedirs(TRASH_DIR, exist_ok=True)

ACTIVE_SERVERS_DAT = os.path.join(BROKERS_DIR, "servers.dat")

INIT_MAX_TRIES = 12
INIT_SLEEP_SEC = 1.5

KNOWN_SUFFIXES = [".t", ".m", ".pro", ".ecn", ".micro", ".mini"]
# ==================

MT5_LOCK = threading.RLock()

USER_LOCKS: Dict[str, threading.Lock] = {}
USER_LOCKS_GUARD = threading.Lock()

SESSION: Dict[str, Any] = {
    "user_id": None,
    "login": None,
    "password": None,  # in-memory only
    "server": None,
    "inst_exe": None,
    "inst_dir": None,
    "last_login_ts": 0.0,
    "last_trade_ts": 0.0,
}


# ---------------------------
# Error shaping (important)
# ---------------------------
@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, str):
        detail = {"message": detail}
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "error": detail.get("error") or "http_error",
            "detail": detail,
            "path": str(request.url.path),
            "worker_port": WORKER_PORT,
            "ts": time.time(),
        },
    )


@app.exception_handler(Exception)
async def unhandled_exc_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": "internal_error",
            "detail": {"message": str(exc), "type": exc.__class__.__name__},
            "path": str(request.url.path),
            "worker_port": WORKER_PORT,
            "ts": time.time(),
        },
    )


class LoginBody(BaseModel):
    mt5_account_id: Optional[str] = None
    login: int
    password: str
    server: Optional[str] = None
    timeout_ms: Optional[int] = 60000


def get_user_lock(user_id: str) -> threading.Lock:
    with USER_LOCKS_GUARD:
        lk = USER_LOCKS.get(user_id)
        if lk is None:
            lk = threading.Lock()
            USER_LOCKS[user_id] = lk
        return lk


def sanitize_user_id(user_id: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9._-]", "_", user_id)


def ensure_base_exists():
    if not os.path.isdir(BASE_MT5_DIR):
        raise HTTPException(500, {"error": "base_missing", "message": f"Base MT5 dir not found: {BASE_MT5_DIR}"})
    if not (
        os.path.isfile(os.path.join(BASE_MT5_DIR, "terminal64.exe"))
        or os.path.isfile(os.path.join(BASE_MT5_DIR, "terminal.exe"))
    ):
        raise HTTPException(500, {"error": "base_missing", "message": f"No terminal exe found in {BASE_MT5_DIR}"})


def user_root_dir(user_id: str) -> str:
    return os.path.join(INSTANCES_ROOT, sanitize_user_id(user_id))


def instance_dir_for_user(user_id: str) -> str:
    return os.path.join(INSTANCES_ROOT, sanitize_user_id(user_id), str(WORKER_PORT))


def ensure_instance_dir_for_user(user_id: str) -> str:
    ensure_base_exists()

    inst_dir = instance_dir_for_user(user_id)
    if not os.path.isdir(inst_dir):
        os.makedirs(os.path.dirname(inst_dir), exist_ok=True)
        shutil.copytree(BASE_MT5_DIR, inst_dir)

    cfg_dir = os.path.join(inst_dir, "config")
    os.makedirs(cfg_dir, exist_ok=True)

    if os.path.isfile(ACTIVE_SERVERS_DAT):
        shutil.copy2(ACTIVE_SERVERS_DAT, os.path.join(cfg_dir, "servers.dat"))
        bases_dir = os.path.join(inst_dir, "bases")
        bases_dir2 = os.path.join(inst_dir, "Bases")
        os.makedirs(bases_dir, exist_ok=True)
        os.makedirs(bases_dir2, exist_ok=True)
        shutil.copy2(ACTIVE_SERVERS_DAT, os.path.join(bases_dir, "servers.dat"))
        shutil.copy2(ACTIVE_SERVERS_DAT, os.path.join(bases_dir2, "servers.dat"))

    base_common = os.path.join(BASE_MT5_DIR, "config", "common.ini")
    if os.path.isfile(base_common):
        shutil.copy2(base_common, os.path.join(cfg_dir, "common.ini"))

    exe64 = os.path.join(inst_dir, "terminal64.exe")
    exe32 = os.path.join(inst_dir, "terminal.exe")
    if os.path.isfile(exe64):
        return exe64
    if os.path.isfile(exe32):
        return exe32
    raise HTTPException(500, {"error": "terminal_missing", "message": f"terminal exe not found in {inst_dir}"})


def _find_terminal_process(inst_exe: Optional[str], inst_dir: Optional[str]):
    if psutil is None:
        return None

    target_exe = (os.path.abspath(inst_exe).lower() if inst_exe else None)
    target_dir = (os.path.abspath(inst_dir).lower() if inst_dir else None)

    for p in psutil.process_iter(["pid", "name", "exe", "cwd", "cmdline"]):
        try:
            name = (p.info.get("name") or "").lower()
            if name not in ("terminal64.exe", "terminal.exe"):
                continue

            exe = (p.info.get("exe") or "").strip()
            cwd = (p.info.get("cwd") or "").strip()

            if target_exe and exe and os.path.abspath(exe).lower() == target_exe:
                return p
            if target_dir and ((exe and os.path.abspath(exe).lower().startswith(target_dir)) or (cwd and os.path.abspath(cwd).lower().startswith(target_dir))):
                return p
        except Exception:
            continue

    return None


def kill_processes_for_dir(inst_dir: str):
    inst_dir_abs = os.path.abspath(inst_dir).lower()

    mt5_targets = {"terminal64.exe", "terminal.exe", "metaeditor.exe", "metaeditor64.exe"}
    webview_targets = {"msedgewebview2.exe", "msedgebroker.exe", "msedge.exe"}

    if psutil is not None:
        targets = []
        for p in psutil.process_iter(["pid", "name", "exe", "cwd", "cmdline"]):
            try:
                name = (p.info.get("name") or "").lower()
                exe = (p.info.get("exe") or "").strip()
                cwd = (p.info.get("cwd") or "").strip()
                cmdline = " ".join(p.info.get("cmdline") or []).lower()

                match = False
                if name in mt5_targets:
                    if exe and os.path.abspath(exe).lower().startswith(inst_dir_abs):
                        match = True
                    if cwd and os.path.abspath(cwd).lower().startswith(inst_dir_abs):
                        match = True

                if not match and name in webview_targets:
                    if inst_dir_abs in cmdline:
                        match = True

                if match:
                    targets.append(p)
            except Exception:
                continue

        for p in targets:
            try:
                p.terminate()
            except Exception:
                pass

        try:
            _, alive = psutil.wait_procs(targets, timeout=4.0)
        except Exception:
            alive = targets

        for p in alive:
            try:
                p.kill()
            except Exception:
                pass

        try:
            psutil.wait_procs(alive, timeout=3.0)
        except Exception:
            pass

        time.sleep(0.4)
        return

    like_pattern = inst_dir_abs.replace("\\", "\\\\")
    ps_cmd = (
        "powershell -NoProfile -Command "
        "\"Get-CimInstance Win32_Process | "
        "Where-Object { "
        "($_.Name -ieq 'terminal64.exe' -or $_.Name -ieq 'terminal.exe' "
        " -or $_.Name -ieq 'metaeditor.exe' -or $_.Name -ieq 'metaeditor64.exe' "
        " -or $_.Name -ieq 'msedgewebview2.exe' -or $_.Name -ieq 'msedgebroker.exe' "
        " -or $_.Name -ieq 'msedge.exe') -and "
        f"((($_.ExecutablePath) -like '*{like_pattern}*') -or (($_.CommandLine) -like '*{like_pattern}*')) "
        "} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}\""
    )
    subprocess.run(ps_cmd, shell=True, check=False)
    time.sleep(0.8)


def wait_no_terminals_for_dir(inst_dir: str, timeout_sec: float = 12.0) -> bool:
    if psutil is None:
        time.sleep(1.0)
        return True

    inst_dir_abs = os.path.abspath(inst_dir).lower()
    deadline = time.time() + timeout_sec

    while time.time() < deadline:
        found = False
        for p in psutil.process_iter(["name", "exe", "cwd"]):
            try:
                name = (p.info.get("name") or "").lower()
                if name not in ("terminal64.exe", "terminal.exe"):
                    continue

                exe = (p.info.get("exe") or "").strip()
                cwd = (p.info.get("cwd") or "").strip()

                if (exe and os.path.abspath(exe).lower().startswith(inst_dir_abs)) or (
                    cwd and os.path.abspath(cwd).lower().startswith(inst_dir_abs)
                ):
                    found = True
                    break
            except Exception:
                continue

        if not found:
            return True

        time.sleep(0.3)

    return False


def try_delete_user_root(user_id: str):
    uroot = user_root_dir(user_id)
    if os.path.isdir(uroot) and not os.listdir(uroot):
        try:
            os.rmdir(uroot)
        except OSError:
            pass


def delete_instance_dir_for_user(user_id: str):
    inst_dir = instance_dir_for_user(user_id)

    with MT5_LOCK:
        try:
            MT5.shutdown()
        except Exception:
            pass

    kill_processes_for_dir(inst_dir)
    wait_no_terminals_for_dir(inst_dir, timeout_sec=8.0)

    for _ in range(4):
        if not os.path.isdir(inst_dir):
            break
        try:
            shutil.rmtree(inst_dir)
            break
        except Exception:
            time.sleep(1.0)

    try_delete_user_root(user_id)


def delete_all_user_ports(user_id: str):
    uroot = user_root_dir(user_id)
    if not os.path.isdir(uroot):
        return

    for name in os.listdir(uroot):
        sub = os.path.join(uroot, name)
        if os.path.isdir(sub):
            kill_processes_for_dir(sub)
            wait_no_terminals_for_dir(sub, timeout_sec=8.0)
            try:
                shutil.rmtree(sub)
            except Exception:
                for _ in range(3):
                    time.sleep(1.0)
                    try:
                        shutil.rmtree(sub)
                        break
                    except Exception:
                        pass

    try_delete_user_root(user_id)


def trash_janitor():
    while True:
        time.sleep(30)
        for name in os.listdir(TRASH_DIR):
            full = os.path.join(TRASH_DIR, name)
            if not os.path.isdir(full):
                continue
            kill_processes_for_dir(full)
            wait_no_terminals_for_dir(full, timeout_sec=6.0)
            try:
                shutil.rmtree(full)
            except Exception:
                pass


@app.on_event("startup")
def on_startup():
    threading.Thread(target=trash_janitor, daemon=True).start()


@app.get("/metrics")
def metrics():
    return {"ok": True, "worker_port": WORKER_PORT, "session_user": SESSION.get("user_id")}


@app.get("/mt5/status")
def mt5_status():
    inst_exe = SESSION.get("inst_exe")
    inst_dir = SESSION.get("inst_dir")

    proc = _find_terminal_process(inst_exe, inst_dir)
    terminal_running = bool(proc)
    terminal_pid = int(proc.pid) if proc else None

    with MT5_LOCK:
        info = None
        last_err = None
        try:
            info = MT5.account_info()
            if info is None:
                last_err = MT5.last_error()
        except Exception:
            last_err = MT5.last_error()

    return {
        "ok": True,
        "worker_port": WORKER_PORT,
        "session_user": SESSION.get("user_id"),
        "session_login": SESSION.get("login"),
        "session_server": SESSION.get("server"),
        "inst_dir": inst_dir,
        "inst_exe": inst_exe,
        "terminal_running": terminal_running,
        "terminal_pid": terminal_pid,
        "mt5_logged_in": bool(info),
        "login": getattr(info, "login", None) if info else None,
        "server": getattr(info, "server", None) if info else None,
        "balance": getattr(info, "balance", None) if info else None,
        "last_error": last_err,
        "last_login_ts": SESSION.get("last_login_ts"),
        "last_trade_ts": SESSION.get("last_trade_ts"),
        "now": time.time(),
    }


@app.post("/upload-server-dat")
async def upload_server_dat(file: UploadFile = File(...)):
    os.makedirs(BROKERS_DIR, exist_ok=True)
    data = await file.read()
    with open(ACTIVE_SERVERS_DAT, "wb") as f:
        f.write(data)
    return {"ok": True, "saved_as": ACTIVE_SERVERS_DAT, "worker_port": WORKER_PORT}


@app.get("/mt5/account-info")
def get_account_info():
    with MT5_LOCK:
        info = MT5.account_info()
        if info is None:
            raise HTTPException(400, {"error": "mt5_not_logged_in", "last_error": MT5.last_error()})
        return {"login": info.login, "name": info.name, "server": info.server, "currency": info.currency, "balance": info.balance}


@app.get("/mt5/list-symbols")
def list_symbols():
    with MT5_LOCK:
        syms = MT5.symbols_get()
        if syms is None:
            raise HTTPException(500, {"error": "symbols_get_failed", "last_error": MT5.last_error()})
        out = [{"name": s.name, "path": s.path, "description": s.description, "visible": s.visible} for s in syms]
        return {"ok": True, "symbols": out}


@app.get("/mt5/tick")
def get_tick(symbol: str):
    if not symbol:
        raise HTTPException(400, {"error": "missing_symbol", "message": "symbol is required"})
    with MT5_LOCK:
        tick = MT5.symbol_info_tick(symbol)
        if tick is None:
            raise HTTPException(400, {"error": "no_tick", "symbol": symbol, "last_error": MT5.last_error()})
        return {"ok": True, "symbol": symbol, "tick": tick._asdict()}


@app.post("/cleanup-instance")
def cleanup_instance(body: dict = Body(default={})):
    user_id = body.get("user_id")
    if not user_id:
        with MT5_LOCK:
            MT5.shutdown()
        return {"ok": True, "note": "no user_id, shutdown only"}

    lk = get_user_lock(user_id)
    with lk:
        delete_instance_dir_for_user(user_id)
        if SESSION.get("user_id") == user_id:
            SESSION.update({
                "user_id": None,
                "login": None,
                "password": None,
                "server": None,
                "inst_exe": None,
                "inst_dir": None,
                "last_login_ts": 0.0,
                "last_trade_ts": 0.0,
            })

    return {"ok": True}


@app.post("/mt5/logout")
def logout(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "user_id is required"})

    lk = get_user_lock(user_id)
    with lk:
        with MT5_LOCK:
            try:
                if SESSION.get("user_id") == user_id:
                    MT5.shutdown()
            except Exception:
                pass

        delete_all_user_ports(user_id)

        if SESSION.get("user_id") == user_id:
            SESSION.update({
                "user_id": None,
                "login": None,
                "password": None,
                "server": None,
                "inst_exe": None,
                "inst_dir": None,
                "last_login_ts": 0.0,
                "last_trade_ts": 0.0,
            })

    return {"ok": True, "logged_out": True, "user_id": user_id}


def _prelaunch_terminal_portable(inst_exe: str) -> bool:
    inst_dir = os.path.dirname(inst_exe)

    # If terminal already running for this dir, do nothing.
    if _find_terminal_process(inst_exe, inst_dir) is not None:
        return False

    try:
        subprocess.Popen([inst_exe, "/portable"], cwd=inst_dir)
        return True
    except Exception:
        return False


def _ensure_logged_in(user_id: str) -> None:
    with MT5_LOCK:
        info = MT5.account_info()
        if info is not None:
            return

        if SESSION.get("user_id") != user_id or not SESSION.get("inst_exe") or not SESSION.get("login") or not SESSION.get("password"):
            raise HTTPException(409, {"error": "mt5_not_logged_in", "last_error": MT5.last_error()})

        proc = _find_terminal_process(SESSION["inst_exe"], SESSION.get("inst_dir"))
        if not proc:
            _prelaunch_terminal_portable(SESSION["inst_exe"])
            time.sleep(1.2)

        last_err = None
        for _ in range(1, INIT_MAX_TRIES + 1):
            ok = MT5.initialize(
                path=SESSION["inst_exe"],
                login=int(SESSION["login"]),
                password=str(SESSION["password"]),
                server=SESSION.get("server"),
                timeout=60000,
            )
            if ok and MT5.account_info() is not None:
                SESSION["last_login_ts"] = time.time()
                return
            last_err = MT5.last_error()
            time.sleep(INIT_SLEEP_SEC)

        raise HTTPException(409, {"error": "mt5_reconnect_failed", "last_error": last_err})


@app.post("/mt5/open-and-login")
def open_and_login(body: LoginBody):
    if not body.mt5_account_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "mt5_account_id (user_id) is required"})

    user_id = body.mt5_account_id
    lk = get_user_lock(user_id)

    with lk:
        inst_exe = ensure_instance_dir_for_user(user_id)
        inst_dir = os.path.dirname(inst_exe)

        with MT5_LOCK:
            try:
                MT5.shutdown()
            except Exception:
                pass

        kill_processes_for_dir(inst_dir)
        wait_no_terminals_for_dir(inst_dir, timeout_sec=12.0)

        # Try initialize FIRST. Only prelaunch terminal if needed (prevents double terminals).
        last_err = None
        init_ok = False
        launched = False

        with MT5_LOCK:
            for _ in range(1, INIT_MAX_TRIES + 1):
                init_ok = MT5.initialize(
                    path=inst_exe,
                    login=body.login,
                    password=body.password,
                    server=body.server,
                    timeout=body.timeout_ms or 60000,
                )

                if init_ok:
                    break

                last_err = MT5.last_error()

                # If MT5 still isn't coming up AND no terminal exists yet, launch once as fallback
                if not launched and _find_terminal_process(inst_exe, inst_dir) is None:
                    launched = _prelaunch_terminal_portable(inst_exe)

                time.sleep(INIT_SLEEP_SEC)

        if not init_ok:
            delete_instance_dir_for_user(user_id)
            raise HTTPException(401, {
                "error": "mt5_initialize_failed",
                "message": "MT5.initialize failed (bad creds/server or terminal not ready)",
                "last_error": last_err,
            })

        info = None
        with MT5_LOCK:
            info = MT5.account_info()

        if info is None:
            for _ in range(6):
                time.sleep(1.5)
                with MT5_LOCK:
                    info = MT5.account_info()
                if info is not None:
                    break

        if info is None:
            delete_instance_dir_for_user(user_id)
            raise HTTPException(401, {
                "error": "mt5_auth_failed",
                "message": "MT5 account_info() is None after initialize (auth failed or server wrong)",
                "last_error": MT5.last_error(),
            })

        SESSION.update({
            "user_id": user_id,
            "login": int(body.login),
            "password": str(body.password),
            "server": body.server,
            "inst_exe": inst_exe,
            "inst_dir": inst_dir,
            "last_login_ts": time.time(),
            "last_trade_ts": SESSION.get("last_trade_ts", 0.0),
        })

        return {
            "ok": True,
            "instance_dir": inst_dir,
            "account": {"login": info.login, "name": info.name, "server": info.server, "balance": info.balance},
        }


def pick_tradable_symbol(requested: str) -> str:
    requested = requested.strip()
    with MT5_LOCK:
        info = MT5.symbol_info(requested)
        if info is not None and info.trade_mode != MT5.SYMBOL_TRADE_MODE_DISABLED:
            return requested

        for suf in KNOWN_SUFFIXES:
            cand = requested + suf
            info = MT5.symbol_info(cand)
            if info is not None and info.trade_mode != MT5.SYMBOL_TRADE_MODE_DISABLED:
                return cand

        all_syms = MT5.symbols_get()
        if all_syms:
            req_low = requested.lower()
            best = None
            for s in all_syms:
                name = s.name
                if not name.lower().startswith(req_low):
                    continue
                sinfo = MT5.symbol_info(name)
                if sinfo is None or sinfo.trade_mode == MT5.SYMBOL_TRADE_MODE_DISABLED:
                    continue
                if best is None or len(name) < len(best):
                    best = name
            if best:
                return best

    return requested


def _quantize_volume(vol: float, step: float) -> float:
    if step <= 0:
        return vol
    steps = round(vol / step)
    return max(steps * step, 0.0)


def _pick_filling_mode(sym) -> int:
    candidates = [MT5.ORDER_FILLING_FOK, MT5.ORDER_FILLING_IOC, MT5.ORDER_FILLING_RETURN]
    try:
        fm = int(getattr(sym, "filling_mode", 0))
    except Exception:
        fm = 0
    for m in candidates:
        if fm == 0 or (fm & m) == m:
            return m
    return MT5.ORDER_FILLING_FOK


def _digits_round(price: float, digits: int) -> float:
    if digits is None or digits < 0:
        return price
    fmt = "{:0." + str(int(digits)) + "f}"
    return float(fmt.format(price))


def _mt5_last_error():
    try:
        return MT5.last_error()
    except Exception:
        return None


def _retcode_name(retcode: int) -> str:
    try:
        for k, v in MT5.__dict__.items():
            if k.startswith("TRADE_RETCODE_") and isinstance(v, int) and v == retcode:
                return k
    except Exception:
        pass
    return "UNKNOWN"


def _parse_float(x, field: str) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        raise HTTPException(400, {"error": "invalid_param", "field": field, "message": f"{field} must be numeric"})


def _validate_sl_tp_for_entry(
    side: str,
    entry_price: float,
    sl: Optional[float],
    tp: Optional[float],
    min_dist_points: int,
    point: float,
) -> List[dict]:
    reasons = []

    def dist_ok(a: float, b: float) -> bool:
        if point <= 0:
            return True
        d_points = abs(a - b) / point
        return d_points >= float(min_dist_points)

    if sl is not None:
        if side == "buy" and not (sl < entry_price):
            reasons.append({"code": "sl_side_invalid", "message": "For BUY, SL must be below entry price", "sl": sl, "entry": entry_price})
        if side == "sell" and not (sl > entry_price):
            reasons.append({"code": "sl_side_invalid", "message": "For SELL, SL must be above entry price", "sl": sl, "entry": entry_price})
        if min_dist_points > 0 and not dist_ok(entry_price, sl):
            reasons.append({"code": "sl_too_close", "message": "SL too close to entry (violates stops level)", "min_dist_points": min_dist_points})

    if tp is not None:
        if side == "buy" and not (tp > entry_price):
            reasons.append({"code": "tp_side_invalid", "message": "For BUY, TP must be above entry price", "tp": tp, "entry": entry_price})
        if side == "sell" and not (tp < entry_price):
            reasons.append({"code": "tp_side_invalid", "message": "For SELL, TP must be below entry price", "tp": tp, "entry": entry_price})
        if min_dist_points > 0 and not dist_ok(tp, entry_price):
            reasons.append({"code": "tp_too_close", "message": "TP too close to entry (violates stops level)", "min_dist_points": min_dist_points})

    return reasons


def _validate_pending_price_rules(
    order_type: str,
    tick_bid: float,
    tick_ask: float,
    price: Optional[float],
    stop_price: Optional[float],
    min_dist_points: int,
    point: float,
) -> List[dict]:
    reasons = []

    def dist_ok(market: float, p: float) -> bool:
        if point <= 0 or min_dist_points <= 0:
            return True
        return (abs(p - market) / point) >= float(min_dist_points)

    if order_type in ("buy_limit", "sell_limit", "buy_stop", "sell_stop"):
        if price is None:
            reasons.append({"code": "missing_price", "message": f"{order_type} requires price"})
            return reasons

    if order_type in ("buy_stop_limit", "sell_stop_limit"):
        if stop_price is None or price is None:
            reasons.append({"code": "missing_price", "message": f"{order_type} requires both stop_price and price (limit price)"})
            return reasons

    if order_type == "buy_limit":
        if not (price < tick_ask):
            reasons.append({"code": "price_side_invalid", "message": "buy_limit price must be below current ask", "ask": tick_ask, "price": price})
        elif not dist_ok(tick_ask, price):
            reasons.append({"code": "price_too_close", "message": "buy_limit price too close to ask (stops level)", "min_dist_points": min_dist_points})

    if order_type == "sell_limit":
        if not (price > tick_bid):
            reasons.append({"code": "price_side_invalid", "message": "sell_limit price must be above current bid", "bid": tick_bid, "price": price})
        elif not dist_ok(tick_bid, price):
            reasons.append({"code": "price_too_close", "message": "sell_limit price too close to bid (stops level)", "min_dist_points": min_dist_points})

    if order_type == "buy_stop":
        if not (price > tick_ask):
            reasons.append({"code": "price_side_invalid", "message": "buy_stop price must be above current ask", "ask": tick_ask, "price": price})
        elif not dist_ok(tick_ask, price):
            reasons.append({"code": "price_too_close", "message": "buy_stop price too close to ask (stops level)", "min_dist_points": min_dist_points})

    if order_type == "sell_stop":
        if not (price < tick_bid):
            reasons.append({"code": "price_side_invalid", "message": "sell_stop price must be below current bid", "bid": tick_bid, "price": price})
        elif not dist_ok(tick_bid, price):
            reasons.append({"code": "price_too_close", "message": "sell_stop price too close to bid (stops level)", "min_dist_points": min_dist_points})

    # stop-limit rules:
    if order_type == "buy_stop_limit":
        if not (stop_price > tick_ask):
            reasons.append({"code": "stop_side_invalid", "message": "buy_stop_limit stop_price must be above ask", "ask": tick_ask, "stop_price": stop_price})
        elif not dist_ok(tick_ask, stop_price):
            reasons.append({"code": "stop_too_close", "message": "buy_stop_limit stop_price too close to ask (stops level)", "min_dist_points": min_dist_points})
        if not (price <= stop_price):
            reasons.append({"code": "limit_vs_stop_invalid", "message": "buy_stop_limit price (limit) should be <= stop_price", "price": price, "stop_price": stop_price})

    if order_type == "sell_stop_limit":
        if not (stop_price < tick_bid):
            reasons.append({"code": "stop_side_invalid", "message": "sell_stop_limit stop_price must be below bid", "bid": tick_bid, "stop_price": stop_price})
        elif not dist_ok(tick_bid, stop_price):
            reasons.append({"code": "stop_too_close", "message": "sell_stop_limit stop_price too close to bid (stops level)", "min_dist_points": min_dist_points})
        if not (price >= stop_price):
            reasons.append({"code": "limit_vs_stop_invalid", "message": "sell_stop_limit price (limit) should be >= stop_price", "price": price, "stop_price": stop_price})

    return reasons


def _order_type_map(order_type: str) -> Tuple[str, int]:
    ot = (order_type or "").strip().lower()

    if ot in ("buy", "market_buy"):
        return ("market", MT5.ORDER_TYPE_BUY)
    if ot in ("sell", "market_sell"):
        return ("market", MT5.ORDER_TYPE_SELL)

    mapping = {
        "buy_limit": MT5.ORDER_TYPE_BUY_LIMIT,
        "sell_limit": MT5.ORDER_TYPE_SELL_LIMIT,
        "buy_stop": MT5.ORDER_TYPE_BUY_STOP,
        "sell_stop": MT5.ORDER_TYPE_SELL_STOP,
        "buy_stop_limit": getattr(MT5, "ORDER_TYPE_BUY_STOP_LIMIT", None),
        "sell_stop_limit": getattr(MT5, "ORDER_TYPE_SELL_STOP_LIMIT", None),
    }

    if ot in mapping and mapping[ot] is not None:
        return ("pending", int(mapping[ot]))

    if ot in ("buy_stop_limit", "sell_stop_limit") and mapping.get(ot) is None:
        raise HTTPException(400, {"error": "unsupported_order_type", "message": f"{ot} not supported by this MT5 python build"})

    raise HTTPException(400, {"error": "invalid_order_type", "message": f"Unsupported order_type: {order_type}"})


@app.post("/mt5/place-order")
def place_order(body: dict = Body(...)):
    user_id = body.get("user_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "user_id is required"})

    if SESSION.get("user_id") not in (None, user_id):
        raise HTTPException(409, {"error": "session_mismatch", "expected_user": SESSION.get("user_id"), "got_user": user_id})

    symbol_req = body.get("symbol")
    if not symbol_req:
        raise HTTPException(400, {"error": "missing_symbol", "message": "symbol is required"})

    volume = _parse_float(body.get("volume", 0.01), "volume") or 0.01
    order_type_in = str(body.get("order_type", "buy")).lower()

    price_in = _parse_float(body.get("price"), "price")
    stop_price_in = _parse_float(body.get("stop_price"), "stop_price")  # stop trigger for stop-limit
    sl_in = _parse_float(body.get("sl"), "sl")
    tp_in = _parse_float(body.get("tp"), "tp")
    deviation = int(body.get("deviation", 20) or 20)

    comment = body.get("comment") or f"fanout:{symbol_req}"
    try:
        magic = int(body.get("magic", 987654))
    except Exception:
        magic = 987654

    inst_dir = instance_dir_for_user(user_id)
    if not os.path.isdir(inst_dir):
        raise HTTPException(400, {"error": "instance_missing", "message": "user instance not found on this worker"})

    _ensure_logged_in(user_id)
    symbol = pick_tradable_symbol(symbol_req)

    kind, mt5_order_type = _order_type_map(order_type_in)

    with MT5_LOCK:
        sym_info = MT5.symbol_info(symbol)
        if sym_info is None:
            try:
                MT5.symbol_select(symbol, True)
                time.sleep(0.2)
            except Exception:
                pass
            sym_info = MT5.symbol_info(symbol)

        if sym_info is None:
            raise HTTPException(400, {"error": "symbol_not_found", "symbol": symbol, "last_error": _mt5_last_error()})

        if sym_info.trade_mode == MT5.SYMBOL_TRADE_MODE_DISABLED:
            raise HTTPException(400, {"error": "trade_disabled", "symbol": symbol})

        if not sym_info.visible:
            MT5.symbol_select(symbol, True)

        vmin = float(getattr(sym_info, "volume_min", 0.01) or 0.01)
        vmax = float(getattr(sym_info, "volume_max", 100.0) or 100.0)
        vstep = float(getattr(sym_info, "volume_step", 0.01) or 0.01)

        vol_adj = _quantize_volume(max(min(volume, vmax), vmin), vstep)

        tick = MT5.symbol_info_tick(symbol)
        if tick is None:
            raise HTTPException(500, {"error": "no_tick", "symbol": symbol, "last_error": _mt5_last_error()})

        bid = float(getattr(tick, "bid", 0.0) or 0.0)
        ask = float(getattr(tick, "ask", 0.0) or 0.0)

        digits = int(getattr(sym_info, "digits", 5) or 5)
        point = float(getattr(sym_info, "point", 0.0) or 0.0)

        stops_level = int(getattr(sym_info, "trade_stops_level", 0) or 0)
        freeze_level = int(getattr(sym_info, "trade_freeze_level", 0) or 0)
        min_dist_points = max(stops_level, 0)

        def norm_price(p: Optional[float]) -> Optional[float]:
            if p is None:
                return None
            return _digits_round(float(p), digits)

        price = norm_price(price_in)          # pending “limit price” for stop-limit
        stop_price = norm_price(stop_price_in)  # pending “trigger” for stop-limit
        sl = norm_price(sl_in)
        tp = norm_price(tp_in)

        reasons: List[dict] = []

        if kind == "market":
            side = "buy" if mt5_order_type == MT5.ORDER_TYPE_BUY else "sell"
            entry_price = ask if side == "buy" else bid
            entry_price = _digits_round(entry_price, digits)
            reasons += _validate_sl_tp_for_entry(side, entry_price, sl, tp, min_dist_points, point)

        else:
            side = "buy" if order_type_in.startswith("buy") else "sell"
            reasons += _validate_pending_price_rules(order_type_in, bid, ask, price, stop_price, min_dist_points, point)

            entry_ref = stop_price if order_type_in.endswith("_stop_limit") else price
            if entry_ref is not None:
                reasons += _validate_sl_tp_for_entry(side, entry_ref, sl, tp, min_dist_points, point)

        if reasons:
            raise HTTPException(422, {
                "error": "validation_failed",
                "message": "Order rejected before sending to MT5 due to invalid SL/TP/price rules",
                "reasons": reasons,
                "context": {
                    "symbol": symbol,
                    "order_type": order_type_in,
                    "kind": kind,
                    "bid": bid,
                    "ask": ask,
                    "price": price,
                    "stop_price": stop_price,
                    "sl": sl,
                    "tp": tp,
                    "stops_level_points": stops_level,
                    "freeze_level_points": freeze_level,
                    "digits": digits,
                    "point": point,
                    "volume": vol_adj,
                }
            })

        type_filling = _pick_filling_mode(sym_info)

        req: Dict[str, Any] = {
            "action": MT5.TRADE_ACTION_DEAL if kind == "market" else MT5.TRADE_ACTION_PENDING,
            "symbol": symbol,
            "volume": vol_adj,
            "type": mt5_order_type,
            "deviation": deviation,
            "comment": comment,
            "magic": magic,
            "type_time": MT5.ORDER_TIME_GTC,
        }

        if kind == "market":
            req["price"] = _digits_round(ask if mt5_order_type == MT5.ORDER_TYPE_BUY else bid, digits)
            req["type_filling"] = type_filling
        else:
            # ✅ FIXED stop-limit mapping
            if order_type_in in ("buy_stop_limit", "sell_stop_limit"):
                # MT5 expects: price = stop trigger; stoplimit = limit price
                req["price"] = stop_price
                req["stoplimit"] = price
            else:
                req["price"] = price

            req["type_filling"] = MT5.ORDER_FILLING_RETURN

        if sl is not None and sl > 0:
            req["sl"] = float(sl)
        if tp is not None and tp > 0:
            req["tp"] = float(tp)

        result = MT5.order_send(req)

        if result is None:
            raise HTTPException(500, {
                "error": "order_send_none",
                "message": "MT5.order_send returned None",
                "last_error": _mt5_last_error(),
                "request": req,
            })

        SESSION["last_trade_ts"] = time.time()

        retcode = int(getattr(result, "retcode", -1))
        retname = _retcode_name(retcode)

        if retcode == MT5.TRADE_RETCODE_DONE or retcode == getattr(MT5, "TRADE_RETCODE_PLACED", -9999):
            return {
                "ok": True,
                "used_symbol": symbol,
                "kind": kind,
                "order_type": order_type_in,
                "normalized_volume": vol_adj,
                "request": req,
                "retcode": retcode,
                "retcode_name": retname,
                "result": result._asdict(),
            }

        raise HTTPException(422, {
            "error": "order_send_failed",
            "message": "MT5 rejected the order",
            "retcode": retcode,
            "retcode_name": retname,
            "result": result._asdict(),
            "request": req,
            "last_error": _mt5_last_error(),
        })


# =========================================================
# NEW: Modify SL/TP on an OPEN POSITION (does not logout)
# =========================================================
@app.post("/mt5/modify-sltp")
def modify_sltp(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "user_id is required"})
    if SESSION.get("user_id") not in (None, user_id):
        raise HTTPException(409, {"error": "session_mismatch", "expected_user": SESSION.get("user_id"), "got_user": user_id})

    ticket = body.get("ticket") or body.get("position") or body.get("position_ticket")
    if ticket is None:
        raise HTTPException(400, {"error": "missing_ticket", "message": "ticket/position is required"})
    ticket = int(ticket)

    sl = _parse_float(body.get("sl"), "sl")
    tp = _parse_float(body.get("tp"), "tp")
    if sl is None and tp is None:
        raise HTTPException(400, {"error": "missing_sltp", "message": "Provide sl and/or tp"})

    _ensure_logged_in(user_id)

    with MT5_LOCK:
        pos_list = MT5.positions_get(ticket=ticket)
        if not pos_list:
            raise HTTPException(404, {"error": "position_not_found", "ticket": ticket})

        pos = pos_list[0]
        symbol = pos.symbol

        sym_info = MT5.symbol_info(symbol)
        if sym_info is None:
            raise HTTPException(400, {"error": "symbol_not_found", "symbol": symbol, "last_error": _mt5_last_error()})

        digits = int(getattr(sym_info, "digits", 5) or 5)

        if sl is not None:
            sl = _digits_round(sl, digits)
        if tp is not None:
            tp = _digits_round(tp, digits)

        req: Dict[str, Any] = {
            "action": MT5.TRADE_ACTION_SLTP,
            "position": ticket,
            "symbol": symbol,
        }
        if sl is not None:
            req["sl"] = float(sl)
        if tp is not None:
            req["tp"] = float(tp)

        result = MT5.order_send(req)
        if result is None:
            raise HTTPException(500, {"error": "order_send_none", "request": req, "last_error": _mt5_last_error()})

        retcode = int(getattr(result, "retcode", -1))
        if retcode != MT5.TRADE_RETCODE_DONE:
            raise HTTPException(422, {
                "error": "modify_failed",
                "retcode": retcode,
                "retcode_name": _retcode_name(retcode),
                "result": result._asdict(),
                "request": req,
                "last_error": _mt5_last_error(),
            })

        return {"ok": True, "ticket": ticket, "symbol": symbol, "sl": sl, "tp": tp, "result": result._asdict()}


# =========================================================
# NEW: Close an OPEN POSITION (does not logout)
# =========================================================
@app.post("/mt5/close-position")
def close_position(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "user_id is required"})
    if SESSION.get("user_id") not in (None, user_id):
        raise HTTPException(409, {"error": "session_mismatch", "expected_user": SESSION.get("user_id"), "got_user": user_id})

    ticket = body.get("ticket") or body.get("position") or body.get("position_ticket")
    if ticket is None:
        raise HTTPException(400, {"error": "missing_ticket", "message": "ticket/position is required"})
    ticket = int(ticket)

    volume_in = _parse_float(body.get("volume"), "volume")
    deviation = int(body.get("deviation", 20) or 20)
    comment = body.get("comment") or f"close:{ticket}"
    try:
        magic = int(body.get("magic", 987654))
    except Exception:
        magic = 987654

    _ensure_logged_in(user_id)

    with MT5_LOCK:
        pos_list = MT5.positions_get(ticket=ticket)
        if not pos_list:
            raise HTTPException(404, {"error": "position_not_found", "ticket": ticket})

        pos = pos_list[0]
        symbol = pos.symbol

        sym_info = MT5.symbol_info(symbol)
        if sym_info is None:
            raise HTTPException(400, {"error": "symbol_not_found", "symbol": symbol, "last_error": _mt5_last_error()})

        digits = int(getattr(sym_info, "digits", 5) or 5)

        tick = MT5.symbol_info_tick(symbol)
        if tick is None:
            raise HTTPException(500, {"error": "no_tick", "symbol": symbol, "last_error": _mt5_last_error()})

        # BUY position closes with SELL at bid; SELL closes with BUY at ask
        if int(pos.type) == MT5.POSITION_TYPE_BUY:
            close_type = MT5.ORDER_TYPE_SELL
            price = float(tick.bid)
        else:
            close_type = MT5.ORDER_TYPE_BUY
            price = float(tick.ask)

        price = _digits_round(price, digits)
        volume = float(volume_in) if volume_in is not None else float(pos.volume)

        req: Dict[str, Any] = {
            "action": MT5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "position": ticket,
            "volume": volume,
            "type": int(close_type),
            "price": float(price),
            "deviation": deviation,
            "comment": comment,
            "magic": magic,
            "type_time": MT5.ORDER_TIME_GTC,
            "type_filling": MT5.ORDER_FILLING_IOC,
        }

        result = MT5.order_send(req)
        if result is None:
            raise HTTPException(500, {"error": "order_send_none", "request": req, "last_error": _mt5_last_error()})

        retcode = int(getattr(result, "retcode", -1))
        if retcode != MT5.TRADE_RETCODE_DONE:
            raise HTTPException(422, {
                "error": "close_failed",
                "retcode": retcode,
                "retcode_name": _retcode_name(retcode),
                "result": result._asdict(),
                "request": req,
                "last_error": _mt5_last_error(),
            })

        return {"ok": True, "ticket": ticket, "symbol": symbol, "closed_volume": volume, "result": result._asdict()}
