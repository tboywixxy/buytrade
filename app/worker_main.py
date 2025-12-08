# worker_main.py
import os
import shutil
import time
import subprocess
import threading
from typing import Optional, Dict, Any

from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

import MetaTrader5 as MT5
from fastapi import (
    FastAPI,
    HTTPException,
    Body,
    UploadFile,
    File,
    Query,
)

try:
    import psutil
except ImportError:
    psutil = None

app = FastAPI(title="MT5 Worker (single broker file, relaxed check)", version="1.2.0")

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

INIT_MAX_TRIES = 8
INIT_SLEEP_SEC = 2.0
ALLOW_BRUTAL_KILL = True

USER_LOCKS: Dict[str, threading.Lock] = {}
USER_LOCKS_GUARD = threading.Lock()

KNOWN_SUFFIXES = [".t", ".m", ".pro", ".ecn", ".micro", ".mini"]
# ==================


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
        raise HTTPException(500, f"Base MT5 dir not found: {BASE_MT5_DIR}")
    if not (
        os.path.isfile(os.path.join(BASE_MT5_DIR, "terminal64.exe"))
        or os.path.isfile(os.path.join(BASE_MT5_DIR, "terminal.exe"))
    ):
        raise HTTPException(500, f"No terminal exe found in {BASE_MT5_DIR}")


def user_root_dir(user_id: str) -> str:
    user_safe = sanitize_user_id(user_id)
    return os.path.join(INSTANCES_ROOT, user_safe)


def instance_dir_for_user(user_id: str) -> str:
    user_safe = sanitize_user_id(user_id)
    return os.path.join(INSTANCES_ROOT, user_safe, str(WORKER_PORT))


def ensure_instance_dir_for_user(user_id: str) -> str:
    ensure_base_exists()

    inst_dir = instance_dir_for_user(user_id)
    if not os.path.isdir(inst_dir):
        os.makedirs(os.path.dirname(inst_dir), exist_ok=True)
        shutil.copytree(BASE_MT5_DIR, inst_dir)

    # config dir
    cfg_dir = os.path.join(inst_dir, "config")
    os.makedirs(cfg_dir, exist_ok=True)

    # copy global servers.dat if we have it
    if os.path.isfile(ACTIVE_SERVERS_DAT):
        shutil.copy2(ACTIVE_SERVERS_DAT, os.path.join(cfg_dir, "servers.dat"))

        # some MT5 look here too
        bases_dir = os.path.join(inst_dir, "bases")
        bases_dir2 = os.path.join(inst_dir, "Bases")
        os.makedirs(bases_dir, exist_ok=True)
        os.makedirs(bases_dir2, exist_ok=True)
        shutil.copy2(ACTIVE_SERVERS_DAT, os.path.join(bases_dir, "servers.dat"))
        shutil.copy2(ACTIVE_SERVERS_DAT, os.path.join(bases_dir2, "servers.dat"))

    # copy common.ini if present
    base_common = os.path.join(BASE_MT5_DIR, "config", "common.ini")
    if os.path.isfile(base_common):
        shutil.copy2(base_common, os.path.join(cfg_dir, "common.ini"))

    exe64 = os.path.join(inst_dir, "terminal64.exe")
    exe32 = os.path.join(inst_dir, "terminal.exe")
    if os.path.isfile(exe64):
        return exe64
    if os.path.isfile(exe32):
        return exe32
    raise HTTPException(500, f"terminal exe not found in {inst_dir}")


def kill_processes_for_dir(inst_dir: str):
    inst_dir_abs = os.path.abspath(inst_dir)

    mt5_targets = {
        "terminal64.exe",
        "terminal.exe",
        "metaeditor.exe",
        "metaeditor64.exe",
    }

    webview_targets = {
        "msedgewebview2.exe",
        "msedgebroker.exe",
        "msedge.exe",
    }

    if psutil is not None:
        killed_any = False
        for p in psutil.process_iter(["pid", "name", "exe", "cwd", "cmdline"]):
            try:
                name = (p.info["name"] or "").lower()
                exe = (p.info.get("exe") or "").strip()
                cwd = (p.info.get("cwd") or "").strip()
                cmdline = " ".join(p.info.get("cmdline") or []).lower()

                match = False

                if name in mt5_targets:
                    if exe and os.path.abspath(exe).startswith(inst_dir_abs):
                        match = True
                    if cwd and os.path.abspath(cwd).startswith(inst_dir_abs):
                        match = True

                if not match and name in webview_targets:
                    if inst_dir_abs.lower() in cmdline:
                        match = True

                if match:
                    p.terminate()
                    killed_any = True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if killed_any:
            time.sleep(1.2)
            return

    # PowerShell fallback
    like_pattern = inst_dir_abs.replace("\\", "\\\\")
    ps_cmd = (
        "powershell -NoProfile -Command "
        "\"Get-CimInstance Win32_Process | "
        "Where-Object { "
        "($_.Name -ieq 'terminal64.exe' -or $_.Name -ieq 'terminal.exe' "
        " -or $_.Name -ieq 'metaeditor.exe' -or $_.Name -ieq 'MetaEditor64.exe' "
        " -or $_.Name -ieq 'msedgewebview2.exe' -or $_.Name -ieq 'msedgebroker.exe' "
        " -or $_.Name -ieq 'msedge.exe') -and "
        f"((($_.ExecutablePath) -like '*{like_pattern}*') -or (($_.CommandLine) -like '*{like_pattern}*')) "
        "} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}\""
    )
    try:
        subprocess.run(ps_cmd, shell=True, check=False)
        time.sleep(1.0)
        return
    except Exception:
        pass

    if ALLOW_BRUTAL_KILL:
        os.system("taskkill /F /IM terminal64.exe /T >NUL 2>&1")
        os.system("taskkill /F /IM terminal.exe /T >NUL 2>&1")
        os.system("taskkill /F /IM metaeditor.exe /T >NUL 2>&1")
        os.system("taskkill /F /IM MetaEditor64.exe /T >NUL 2>&1")
        os.system("taskkill /F /IM msedgewebview2.exe /T >NUL 2>&1")
        os.system("taskkill /F /IM msedgebroker.exe /T >NUL 2>&1")
        os.system("taskkill /F /IM msedge.exe /T >NUL 2>&1")
        time.sleep(1.0)


def try_delete_user_root(user_id: str):
    uroot = user_root_dir(user_id)
    if os.path.isdir(uroot) and not os.listdir(uroot):
        try:
            os.rmdir(uroot)
        except OSError:
            pass


def delete_instance_dir_for_user(user_id: str):
    inst_dir = instance_dir_for_user(user_id)

    try:
        MT5.shutdown()
    except Exception:
        pass

    kill_processes_for_dir(inst_dir)

    for _ in range(3):
        if not os.path.isdir(inst_dir):
            break
        try:
            shutil.rmtree(inst_dir)
            break
        except Exception:
            time.sleep(1.0)

    try_delete_user_root(user_id)


def trash_janitor():
    while True:
        time.sleep(30)
        for name in os.listdir(TRASH_DIR):
            full = os.path.join(TRASH_DIR, name)
            if not os.path.isdir(full):
                continue
            kill_processes_for_dir(full)
            try:
                shutil.rmtree(full)
            except Exception:
                pass


@app.on_event("startup")
def on_startup():
    t = threading.Thread(target=trash_janitor, daemon=True)
    t.start()


@app.get("/metrics")
def metrics():
    return {"ok": True, "worker_port": WORKER_PORT}


@app.post("/upload-server-dat")
async def upload_server_dat(file: UploadFile = File(...)):
    """Every upload REPLACES C:\MT5\brokers\servers.dat"""
    os.makedirs(BROKERS_DIR, exist_ok=True)
    data = await file.read()
    with open(ACTIVE_SERVERS_DAT, "wb") as f:
        f.write(data)
    return {
        "ok": True,
        "saved_as": ACTIVE_SERVERS_DAT,
        "note": "replaced main servers.dat on this worker",
        "worker_port": WORKER_PORT,
    }


@app.get("/brokers/has")
def has_broker(server: str = Query(..., description="MT5 server name to check")):
    if not os.path.isfile(ACTIVE_SERVERS_DAT):
        return {"ok": True, "has": False, "file_exists": False}
    data = None
    try:
        with open(ACTIVE_SERVERS_DAT, "rb") as f:
            data = f.read()
    except Exception:
        return {
            "ok": True,
            "has": False,
            "file_exists": True,
            "note": "servers.dat unreadable but present",
        }
    target = server.encode("utf-8")
    if data and target in data:
        return {"ok": True, "has": True, "file_exists": True, "source": "main"}
    return {
        "ok": True,
        "has": False,
        "file_exists": True,
        "note": "servers.dat present but exact server name not found (binary file)",
    }


@app.get("/mt5/account-info")
def get_account_info():
    info = MT5.account_info()
    if info is None:
        raise HTTPException(400, "no MT5 account_info (not logged in?)")
    return {
        "login": info.login,
        "name": info.name,
        "server": info.server,
        "currency": info.currency,
        "balance": info.balance,
    }


@app.get("/mt5/list-symbols")
def list_symbols():
    syms = MT5.symbols_get()
    if syms is None:
        raise HTTPException(500, f"symbols_get failed: {MT5.last_error()}")
    out = []
    for s in syms:
        out.append({
            "name": s.name,
            "path": s.path,
            "description": s.description,
            "visible": s.visible,
        })
    return {"ok": True, "symbols": out}


@app.post("/cleanup-instance")
def cleanup_instance(body: dict = Body(default={})):
    user_id = body.get("user_id")
    if not user_id:
        MT5.shutdown()
        return {"ok": True, "note": "no user_id, shutdown only"}
    lk = get_user_lock(user_id)
    with lk:
        delete_instance_dir_for_user(user_id)
    return {"ok": True}


@app.post("/mt5/open-and-login")
def open_and_login(body: LoginBody):
    if not body.mt5_account_id:
        raise HTTPException(400, "mt5_account_id (user_id) is required")

    user_id = body.mt5_account_id
    lk = get_user_lock(user_id)

    with lk:
        inst_exe = ensure_instance_dir_for_user(user_id)

        MT5.shutdown()

        last_err = None
        for _ in range(1, INIT_MAX_TRIES + 1):
            ok = MT5.initialize(
                path=inst_exe,
                login=body.login,
                password=body.password,
                server=body.server,
                timeout=body.timeout_ms or 60000,
                portable=True,
            )
            if ok:
                break
            last_err = MT5.last_error()
            time.sleep(INIT_SLEEP_SEC)
        else:
            delete_instance_dir_for_user(user_id)
            raise HTTPException(500, f"MT5 initialize failed: {last_err}")

        info = MT5.account_info()
        if info is None:
            for _ in range(5):
                time.sleep(2.0)
                info = MT5.account_info()
                if info is not None:
                    break

        if info is None:
            delete_instance_dir_for_user(user_id)
            raise HTTPException(500, "MT5 account_info() is None after retries (auth failed)")

        return {
            "ok": True,
            "instance_dir": os.path.dirname(inst_exe),
            "account": {
                "login": info.login,
                "name": info.name,
                "server": info.server,
                "balance": info.balance,
            },
        }


def pick_tradable_symbol(requested: str) -> str:
    requested = requested.strip()
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


# ===== helpers for hardened order send =====
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


@app.post("/mt5/place-order")
def place_order(body: dict = Body(...)):
    user_id = body.get("user_id")
    if not user_id:
        raise HTTPException(400, "user_id is required")

    symbol_req = body.get("symbol")
    if not symbol_req:
        raise HTTPException(400, "symbol is required")

    try:
        volume = float(body.get("volume", 0.01))
    except Exception:
        raise HTTPException(400, "invalid volume")

    order_type = str(body.get("order_type", "buy")).lower()
    sl = body.get("sl", 0.0)
    tp = body.get("tp", 0.0)
    try:
        sl = float(sl) if sl is not None else 0.0
        tp = float(tp) if tp is not None else 0.0
    except Exception:
        raise HTTPException(400, "invalid sl/tp")

    comment = body.get("comment") or f"fanout:{symbol_req}"
    try:
        magic = int(body.get("magic", 987654))
    except Exception:
        magic = 987654

    inst_dir = instance_dir_for_user(user_id)
    if not os.path.isdir(inst_dir):
        raise HTTPException(400, "user instance not found on this worker")

    # 1) Resolve a tradable symbol
    symbol = pick_tradable_symbol(symbol_req)
    sym_info = MT5.symbol_info(symbol)
    if sym_info is None:
        raise HTTPException(400, f"symbol {symbol} not found on broker")

    if sym_info.trade_mode == MT5.SYMBOL_TRADE_MODE_DISABLED:
        raise HTTPException(400, f"symbol {symbol} trade disabled for this account")

    if not sym_info.visible:
        MT5.symbol_select(symbol, True)

    # 2) Normalize volume
    vmin = float(getattr(sym_info, "volume_min", 0.01) or 0.01)
    vmax = float(getattr(sym_info, "volume_max", 100.0) or 100.0)
    vstep = float(getattr(sym_info, "volume_step", 0.01) or 0.01)
    vol_adj = _quantize_volume(max(min(volume, vmax), vmin), vstep)
    if vol_adj < vmin - 1e-12 or vol_adj > vmax + 1e-12:
        raise HTTPException(400, {
            "error": "invalid volume",
            "symbol": symbol,
            "requested": volume,
            "normalized": vol_adj,
            "limits": [vmin, vmax],
            "step": vstep,
        })

    # 3) Current price / digits
    tick = MT5.symbol_info_tick(symbol)
    if tick is None:
        raise HTTPException(500, f"no tick for {symbol} (market may be closed)")

    if order_type == "buy":
        order_type_const = MT5.ORDER_TYPE_BUY
        price = tick.ask
    else:
        order_type_const = MT5.ORDER_TYPE_SELL
        price = tick.bid

    price_digits = int(getattr(sym_info, "digits", 5) or 5)
    price = _digits_round(price, price_digits)

    # 4) Filling mode
    type_filling = _pick_filling_mode(sym_info)

    # 5) Build order
    req = {
        "action": MT5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": vol_adj,
        "type": order_type_const,
        "price": price,
        "deviation": 20,
        "comment": comment,
        "magic": magic,
        "type_time": MT5.ORDER_TIME_GTC,
        "type_filling": type_filling,
    }
    if sl and sl > 0:
        req["sl"] = float(sl)
    if tp and tp > 0:
        req["tp"] = float(tp)

    # 6) Send
    result = MT5.order_send(req)
    if result is None:
        raise HTTPException(500, "order_send returned None")

    if result.retcode == MT5.TRADE_RETCODE_DONE:
        return {
            "ok": True,
            "used_symbol": symbol,
            "normalized_volume": vol_adj,
            "filling": type_filling,
            "result": result._asdict(),
        }

    # 7) Map common retcodes to clearer HTTPs
    ret = int(result.retcode)
    detail = result._asdict()
    RETCLOSE = {
        MT5.TRADE_RETCODE_MARKET_CLOSED,
        MT5.TRADE_RETCODE_NO_PRICES,
        MT5.TRADE_RETCODE_OFFQUOTES,
    }
    RETVOL = {
        MT5.TRADE_RETCODE_INVALID_VOLUME,
        MT5.TRADE_RETCODE_VOLUME_LIMIT,
    }
    RETPRICE = {
        MT5.TRADE_RETCODE_REQUOTE,
        MT5.TRADE_RETCODE_PRICE_CHANGED,
        MT5.TRADE_RETCODE_PRICE_OFF,
    }

    if ret in RETCLOSE:
        raise HTTPException(409, {
            "error": "market closed/off quotes",
            "symbol": symbol,
            "retcode": ret,
            "detail": detail,
        })
    if ret in RETVOL:
        raise HTTPException(400, {
            "error": "invalid volume",
            "symbol": symbol,
            "requested": volume,
            "normalized": vol_adj,
            "limits": [vmin, vmax],
            "step": vstep,
            "retcode": ret,
            "detail": detail,
        })
    if ret in RETPRICE:
        raise HTTPException(409, {
            "error": "price change/requote",
            "symbol": symbol,
            "retcode": ret,
            "detail": detail,
        })

    raise HTTPException(422, {
        "error": "order_send failed",
        "symbol": symbol,
        "retcode": ret,
        "detail": detail,
    })
