# manager_main.py
import os
import subprocess
import time
import threading
import re
import shutil
import json
from pathlib import Path
from typing import Dict, Any, Optional, List

import requests
from requests.exceptions import ReadTimeout, ConnectionError as ReqConnectionError
from fastapi import FastAPI, HTTPException, Body, UploadFile, File, Query

app = FastAPI(title="MT5 Manager (relaxed broker check)", version="1.4.0")

# ====== CONFIG ======
INSTANCES_BASE = r"C:\MT5\instances"
BROKERS_DIR = r"C:\MT5\brokers"
MAIN_SERVERS_DAT = os.path.join(BROKERS_DIR, "servers.dat")

PRESTART_PORTS = list(range(8000, 8019 + 1))  # 8000–8019 prestarted
DYNAMIC_BASE_PORT = 8050
WORKER_HOST = "127.0.0.1"

IDLE_TTL_SEC = 3600
DEEP_CLEAN_INTERVAL_SEC = 3600
REG_CLEAN_INTERVAL_SEC = 3600

# NEW: per-VPS capacity (distinct MT5 accounts/users)
MAX_ACCOUNTS_PER_VPS = 2

SYMBOL_CACHE_PATH = Path(r"C:\MT5\symbol_cache.json")
# SYMBOL_CACHE[user_id] = {"server": "...", "symbols": [names...], "fetched_at": ts}
SYMBOL_CACHE: Dict[str, Dict[str, Any]] = {}

workers: Dict[int, Dict[str, Any]] = {}
user_sessions: Dict[str, Dict[str, Any]] = {}

STATE_LOCK = threading.Lock()
# =====================


def _safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)


def sanitize_user_id(user_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", user_id)


def load_symbol_cache():
    global SYMBOL_CACHE
    if SYMBOL_CACHE_PATH.exists():
        try:
            SYMBOL_CACHE = json.loads(SYMBOL_CACHE_PATH.read_text("utf-8"))
        except Exception:
            SYMBOL_CACHE = {}
    else:
        SYMBOL_CACHE = {}


def save_symbol_cache():
    try:
        SYMBOL_CACHE_PATH.write_text(json.dumps(SYMBOL_CACHE, indent=2), encoding="utf-8")
    except Exception:
        pass


def robust_rmtree(path: str, retries: int = 5, delay: float = 1.0):
    if not path or not os.path.exists(path):
        return
    for _ in range(retries):
        try:
            shutil.rmtree(path)
            return
        except Exception:
            time.sleep(delay)
    shutil.rmtree(path, ignore_errors=True)


def user_instance_path(user_id: str, port: int) -> str:
    user_safe = sanitize_user_id(user_id)
    return os.path.join(INSTANCES_BASE, user_safe, str(port))


def try_delete_user_root(user_id: str):
    user_safe = sanitize_user_id(user_id)
    user_root = os.path.join(INSTANCES_BASE, user_safe)
    if os.path.isdir(user_root) and len(os.listdir(user_root)) == 0:
        try:
            os.rmdir(user_root)
        except Exception:
            pass


def wait_for_worker_ready(port: int, attempts: int = 20, delay: float = 0.5) -> bool:
    url = f"http://{WORKER_HOST}:{port}/metrics"
    for _ in range(attempts):
        try:
            r = requests.get(url, timeout=1.5)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(delay)
    return False


def start_worker_process(port: int):
    os.makedirs(INSTANCES_BASE, exist_ok=True)
    os.makedirs(BROKERS_DIR, exist_ok=True)

    env = os.environ.copy()
    env["MT5_INSTANCES_ROOT"] = INSTANCES_BASE
    env["MT5_BROKERS_DIR"] = BROKERS_DIR
    env["MT5_BASE_DIR"] = r"C:\MT5\mt5-template"
    env["MT5_WORKER_PORT"] = str(port)

    # Use worker_venv Python explicitly
    worker_python = r"C:\MT5\worker_venv\Scripts\python.exe"

    cmd = [
        worker_python, "-m", "uvicorn", "worker_main:app",
        "--host", "0.0.0.0", "--port", str(port),
    ]

    proc = subprocess.Popen(cmd, env=env, cwd=r"C:\MT5\app")
    ok = wait_for_worker_ready(port)
    if not ok:
        try:
            proc.terminate()
        except Exception:
            pass
        raise RuntimeError(f"Worker on port {port} failed to start")

    return proc


def prestart_workers():
    for port in PRESTART_PORTS:
        proc = start_worker_process(port)
        with STATE_LOCK:
            workers[port] = {
                "proc": proc,
                "instances_root": INSTANCES_BASE,
                "current_user": None,
                "last_used": 0.0,
            }


def start_dynamic_worker() -> int:
    # atomically choose a fresh port
    with STATE_LOCK:
        port = max(list(workers.keys()) + [DYNAMIC_BASE_PORT - 1]) + 1 if workers else DYNAMIC_BASE_PORT
    proc = start_worker_process(port)
    with STATE_LOCK:
        workers[port] = {
            "proc": proc,
            "instances_root": INSTANCES_BASE,
            "current_user": None,
            "last_used": 0.0,
        }
    return port


def pick_and_reserve_worker(user_id: str) -> int:
    """Atomically choose a free worker and reserve it for user_id."""
    now = time.time()
    with STATE_LOCK:
        free_ports = [p for p, w in workers.items() if w["current_user"] is None]
        if free_ports:
            free_ports.sort(key=lambda p: workers[p]["last_used"])
            port = free_ports[0]
            workers[port]["current_user"] = user_id
            workers[port]["last_used"] = now
            return port

    port = start_dynamic_worker()
    with STATE_LOCK:
        workers[port]["current_user"] = user_id
        workers[port]["last_used"] = now
    return port


def _assert_worker_owned_by(user_id: str, port: int):
    with STATE_LOCK:
        owner = workers.get(port, {}).get("current_user")
    if owner not in (None, user_id):
        raise HTTPException(409, f"worker {port} is busy with {owner}")


def _call_worker_get(port: int, path: str, timeout=8):
    url = f"http://{WORKER_HOST}:{port}{path}"
    try:
        r = requests.get(url, timeout=timeout)
        if r.headers.get("content-type", "").startswith("application/json"):
            return r.status_code, r.json()
        return r.status_code, {"raw": r.text}
    except Exception as e:
        return None, {"error": str(e)}


def call_worker_open(port: int, user_id: str, login: int, password: str,
                     server: Optional[str], timeout_ms: Optional[int]):
    url = f"http://{WORKER_HOST}:{port}/mt5/open-and-login"
    payload = {
        "mt5_account_id": user_id,
        "login": login,
        "password": password,
        "server": server,
        "timeout_ms": timeout_ms or 60000,
    }

    for _ in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=(5, 120))
            break
        except (ReqConnectionError, ReadTimeout):
            time.sleep(1.0)
    else:
        drop_user(user_id)
        raise HTTPException(502, f"worker on port {port} did not respond")

    if resp.status_code != 200:
        drop_user(user_id)
        # bubble worker error up as JSON if possible
        try:
            raise HTTPException(resp.status_code, resp.json())
        except Exception:
            raise HTTPException(resp.status_code, resp.text)

    return resp.json()


def call_worker_cleanup_user(port: int, user_id: str):
    url = f"http://{WORKER_HOST}:{port}/cleanup-instance"
    try:
        requests.post(url, json={"user_id": user_id}, timeout=4)
    except Exception:
        pass


def fetch_symbols_for_user(port: int, user_id: str, fallback_server: Optional[str]) -> None:
    real_server = fallback_server
    try:
        r = requests.get(f"http://{WORKER_HOST}:{port}/mt5/account-info", timeout=4)
        if r.status_code == 200:
            data = r.json()
            real_server = data.get("server") or fallback_server
    except Exception:
        pass

    symbols: List[str] = []
    try:
        r = requests.get(f"http://{WORKER_HOST}:{port}/mt5/list-symbols", timeout=8)
        if r.status_code == 200:
            data = r.json()
            symbols = [s["name"] for s in data.get("symbols", [])]
    except Exception:
        pass

    if symbols:
        now = time.time()
        SYMBOL_CACHE[user_id] = {
            "server": real_server,
            "symbols": symbols,
            "fetched_at": now,
        }
        save_symbol_cache()

    with STATE_LOCK:
        if user_id in user_sessions and real_server:
            user_sessions[user_id]["server"] = real_server


def get_symbols_for_user(user_id: str, fresh_seconds: int = 900) -> List[str]:
    """Return cached symbols for user if fresh; else try to fetch via worker."""
    now = time.time()
    cache = SYMBOL_CACHE.get(user_id)
    if cache and (now - cache.get("fetched_at", 0)) <= fresh_seconds:
        return list(cache.get("symbols") or [])

    # try to fetch from worker if connected
    with STATE_LOCK:
        sess = user_sessions.get(user_id)
    if not sess:
        return list(cache.get("symbols") or []) if cache else []

    port = sess["port"]
    try:
        r = requests.get(f"http://{WORKER_HOST}:{port}/mt5/list-symbols", timeout=8)
        if r.status_code == 200:
            data = r.json()
            syms = [s["name"] for s in data.get("symbols", [])]
            if syms:
                SYMBOL_CACHE[user_id] = {
                    "server": sess.get("server"),
                    "symbols": syms,
                    "fetched_at": now,
                }
                save_symbol_cache()
                return syms
    except Exception:
        pass
    return list(cache.get("symbols") or []) if cache else []


def resolve_symbol_from_cache(user_id: str, requested: str) -> str:
    """Try to map a short name to actual broker symbol using cached list."""
    req = requested.strip()
    if not req:
        return requested
    syms = get_symbols_for_user(user_id)
    if not syms:
        return requested

    req_low = req.lower()
    # exact (case-insensitive)
    for s in syms:
        if s.lower() == req_low:
            return s
    # startswith → prefer shortest
    candidates = [s for s in syms if s.lower().startswith(req_low)]
    if candidates:
        candidates.sort(key=len)
        return candidates[0]
    # contains (rare fallback)
    contains = [s for s in syms if req_low in s.lower()]
    if contains:
        contains.sort(key=len)
        return contains[0]
    return requested


def drop_user(user_id: str):
    with STATE_LOCK:
        sess = user_sessions.pop(user_id, None)
    if not sess:
        return

    port = sess["port"]
    with STATE_LOCK:
        if port in workers:
            workers[port]["current_user"] = None

    inst_path = user_instance_path(user_id, port)
    if os.path.isdir(inst_path):
        robust_rmtree(inst_path)
    try_delete_user_root(user_id)


def cleanup_job(interval_sec: int, deep: bool = False):
    while True:
        time.sleep(interval_sec)
        now = time.time()

        with STATE_LOCK:
            users = list(user_sessions.keys())

        for user_id in users:
            with STATE_LOCK:
                sess = user_sessions.get(user_id)
                if not sess:
                    continue
                last_seen = sess["last_seen"]
                port = sess["port"]

            idle_time = now - last_seen
            if idle_time > IDLE_TTL_SEC or deep:
                call_worker_cleanup_user(port, user_id)
                drop_user(user_id)


@app.on_event("startup")
def on_startup():
    os.makedirs(INSTANCES_BASE, exist_ok=True)
    os.makedirs(BROKERS_DIR, exist_ok=True)
    load_symbol_cache()

    prestart_workers()

    t1 = threading.Thread(target=cleanup_job, args=(REG_CLEAN_INTERVAL_SEC, False), daemon=True)
    t1.start()

    t2 = threading.Thread(target=cleanup_job, args=(DEEP_CLEAN_INTERVAL_SEC, True), daemon=True)
    t2.start()


# ===== BASIC ENDPOINTS =====

@app.get("/")
def root():
    return {"message": "MT5 Manager API", "status": "running", "version": "1.4.0"}


@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": time.time()}


@app.get("/metrics")
def metrics():
    with STATE_LOCK:
        active_workers = {
            p: {
                "current_user": w["current_user"],
                "last_used": w["last_used"],
            }
            for p, w in workers.items()
        }
        active_sessions = len(user_sessions)

    return {
        "manager": "running",
        "worker_count": len(active_workers),
        "workers": active_workers,
        "active_sessions": active_sessions,        # NEW
        "max_accounts": MAX_ACCOUNTS_PER_VPS,     # NEW
        "timestamp": time.time(),
    }


# ========== UPLOAD (REPLACE MAIN SERVERS.DAT) ==========
@app.post("/upload-server-dat")
async def upload_server_dat(file: UploadFile = File(...)):
    """Every upload replaces C:\MT5\brokers\servers.dat on the manager."""
    os.makedirs(BROKERS_DIR, exist_ok=True)
    raw = await file.read()
    with open(MAIN_SERVERS_DAT, "wb") as f:
        f.write(raw)
    return {"ok": True, "saved_as": MAIN_SERVERS_DAT, "note": "replaced main servers.dat on manager"}


# ===== Manager-side helpers =====
@app.get("/probe/symbols")
def probe_symbols(user_id: str = Query(..., description="user_id to probe")):
    with STATE_LOCK:
        sess = user_sessions.get(user_id)
    if not sess:
        raise HTTPException(404, "user not connected")
    port = sess["port"]
    status, data = _call_worker_get(port, "/mt5/list-symbols", timeout=8)
    if status is None:
        raise HTTPException(502, f"worker {port} unreachable: {data}")
    return {"worker_port": port, "status": status, "data": data}


@app.get("/symbols")
def list_symbols(
    user_id: str = Query(..., description="user_id whose symbols to list"),
    prefix: Optional[str] = Query(None, description="optional prefix filter (case-insensitive)")
):
    syms = get_symbols_for_user(user_id)
    if prefix:
        p = prefix.lower()
        syms = [s for s in syms if s.lower().startswith(p)]
    return {"ok": True, "count": len(syms), "symbols": syms}


@app.post("/connect")
def connect(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("userId") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, "Missing user_id / userId / mt5_account_id in JSON body.")

    if "login" not in body or "password" not in body:
        raise HTTPException(400, "login and password are required")

    login = int(body["login"])
    password = body["password"]
    server = body.get("server")
    timeout_ms = body.get("timeout_ms", 60000)

    with STATE_LOCK:
        sess = user_sessions.get(user_id)

    # Reuse existing session
    if sess:
        port = sess["port"]
        _assert_worker_owned_by(user_id, port)

        if server:
            try:
                chk = requests.get(f"http://{WORKER_HOST}:{port}/brokers/has",
                                   params={"server": server}, timeout=3)
                if chk.status_code == 200:
                    data = chk.json()
                    if data.get("file_exists") is False:
                        raise HTTPException(400, "server not available on this VPS. please upload server file.")
                else:
                    raise HTTPException(400, "could not verify broker on this VPS. please upload server file.")
            except Exception:
                raise HTTPException(400, "could not verify broker on this VPS. please upload server file.")

        result = call_worker_open(port, user_id, login, password, server, timeout_ms)

        now = time.time()
        with STATE_LOCK:
            user_sessions[user_id]["last_seen"] = now
            if port in workers:
                workers[port]["last_used"] = now

        fetch_symbols_for_user(port, user_id, server)
        return {"ok": True, "reused": True, "worker_port": port, "result": result}

    # NEW: per-VPS capacity check for NEW users
    with STATE_LOCK:
        current_active = len(user_sessions)
    if current_active >= MAX_ACCOUNTS_PER_VPS:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "instance_full",
                "active_sessions": current_active,
                "max_accounts": MAX_ACCOUNTS_PER_VPS,
            },
        )

    # New user: atomically pick & reserve a worker
    port = pick_and_reserve_worker(user_id)
    now = time.time()
    with STATE_LOCK:
        user_sessions[user_id] = {"port": port, "last_seen": now, "server": server}

    if server:
        try:
            chk = requests.get(f"http://{WORKER_HOST}:{port}/brokers/has",
                               params={"server": server}, timeout=3)
            if chk.status_code == 200:
                data = chk.json()
                if data.get("file_exists") is False:
                    drop_user(user_id)
                    raise HTTPException(400, "server not available on this VPS. please upload server file.")
            else:
                drop_user(user_id)
                raise HTTPException(400, "could not verify broker on this VPS. please upload server file.")
        except Exception:
            drop_user(user_id)
            raise HTTPException(400, "could not verify broker on this VPS. please upload server file.")

    try:
        result = call_worker_open(port, user_id, login, password, server, timeout_ms)
    except HTTPException as e:
        drop_user(user_id)
        raise e

    fetch_symbols_for_user(port, user_id, server)
    return {"ok": True, "reused": False, "worker_port": port, "result": result}


def ensure_user_connected(user_id: str,
                          login: Optional[int] = None,
                          password: Optional[str] = None,
                          server: Optional[str] = None,
                          timeout_ms: Optional[int] = 60000) -> int:
    now = time.time()
    with STATE_LOCK:
        sess = user_sessions.get(user_id)

    if sess:
        with STATE_LOCK:
            user_sessions[user_id]["last_seen"] = now
        return sess["port"]

    # New session path: enforce capacity here as well
    with STATE_LOCK:
        current_active = len(user_sessions)
    if current_active >= MAX_ACCOUNTS_PER_VPS:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "instance_full",
                "active_sessions": current_active,
                "max_accounts": MAX_ACCOUNTS_PER_VPS,
            },
        )

    if login is None or password is None:
        raise HTTPException(400, "user not connected and no login/password provided")

    port = pick_and_reserve_worker(user_id)
    with STATE_LOCK:
        user_sessions[user_id] = {"port": port, "last_seen": now, "server": server}

    if server:
        try:
            chk = requests.get(f"http://{WORKER_HOST}:{port}/brokers/has",
                               params={"server": server}, timeout=3)
            if chk.status_code == 200:
                data = chk.json()
                if data.get("file_exists") is False:
                    drop_user(user_id)
                    raise HTTPException(400, "server not available on this VPS. please upload server file.")
            else:
                drop_user(user_id)
                raise HTTPException(400, "could not verify broker on this VPS. please upload server file.")
        except Exception:
            drop_user(user_id)
            raise HTTPException(400, "could not verify broker on this VPS. please upload server file.")

    try:
        _ = call_worker_open(port, user_id, login, password, server, timeout_ms)
    except HTTPException as e:
        drop_user(user_id)
        raise e

    fetch_symbols_for_user(port, user_id, server)
    return port


def call_worker_place_order(port: int, payload: dict) -> dict:
    url = f"http://{WORKER_HOST}:{port}/mt5/place-order"
    r = requests.post(url, json=payload, timeout=8)

    # Try to decode JSON, but fall back to raw text
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}

    if r.status_code != 200:
        # surface worker error payload as-is
        raise HTTPException(r.status_code, data)

    return data


@app.post("/place-trade")
def place_trade(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, "user_id is required")

    symbol_req = body.get("symbol")
    if not symbol_req:
        raise HTTPException(400, "symbol is required")

    volume = body.get("volume", 0.01)
    order_type = body.get("order_type", "buy")
    sl = body.get("sl")
    tp = body.get("tp")
    comment = body.get("comment")
    magic = body.get("magic")

    login = body.get("login")
    password = body.get("password")
    server = body.get("server")
    timeout_ms = body.get("timeout_ms", 60000)

    port = ensure_user_connected(
        user_id=user_id,
        login=login,
        password=password,
        server=server,
        timeout_ms=timeout_ms,
    )

    # Try to upgrade symbol to broker's actual name using cache (best effort)
    resolved = resolve_symbol_from_cache(user_id, symbol_req)

    worker_payload = {
        "user_id": user_id,
        "symbol": resolved,
        "volume": volume,
        "order_type": order_type,
    }
    if sl is not None:
        worker_payload["sl"] = sl
    if tp is not None:
        worker_payload["tp"] = tp
    if comment is not None:
        worker_payload["comment"] = comment
    if magic is not None:
        worker_payload["magic"] = magic

    result = call_worker_place_order(port, worker_payload)

    now = time.time()
    with STATE_LOCK:
        if user_id in user_sessions:
            user_sessions[user_id]["last_seen"] = now
        if port in workers:
            workers[port]["last_used"] = now

    return {
        "ok": True,
        "worker_port": port,
        "requested_symbol": symbol_req,
        "resolved_symbol": resolved,
        "result": result,
    }


# ---- Run directly (dev) ----
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
