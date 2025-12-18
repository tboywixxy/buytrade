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
from fastapi import FastAPI, HTTPException, Body, Query, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="MT5 Manager (stable workers + reuse)", version="1.8.0")

# ====== CONFIG ======
INSTANCES_BASE = r"C:\MT5\instances"
BROKERS_DIR = r"C:\MT5\brokers"

PRESTART_PORTS = list(range(8000, 8019 + 1))
DYNAMIC_BASE_PORT = 8050
WORKER_HOST = "127.0.0.1"

IDLE_TTL_SEC = int(os.getenv("IDLE_TTL_SEC", "3600"))
REG_CLEAN_INTERVAL_SEC = int(os.getenv("REG_CLEAN_INTERVAL_SEC", "300"))
MAX_ACCOUNTS_PER_VPS = int(os.getenv("MAX_ACCOUNTS_PER_VPS", "50"))

SYMBOL_CACHE_PATH = Path(r"C:\MT5\symbol_cache.json")
SYMBOL_CACHE: Dict[str, Dict[str, Any]] = {}

workers: Dict[int, Dict[str, Any]] = {}
user_sessions: Dict[str, Dict[str, Any]] = {}

STATE_LOCK = threading.Lock()

# per-user lock (prevents concurrent connect/open for same user)
USER_LOCKS: Dict[str, threading.Lock] = {}
USER_LOCKS_GUARD = threading.Lock()


def get_user_lock(user_id: str) -> threading.Lock:
    with USER_LOCKS_GUARD:
        lk = USER_LOCKS.get(user_id)
        if lk is None:
            lk = threading.Lock()
            USER_LOCKS[user_id] = lk
        return lk


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
            "ts": time.time(),
        },
    )


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


def worker_metrics(port: int) -> Optional[dict]:
    url = f"http://{WORKER_HOST}:{port}/metrics"
    try:
        r = requests.get(url, timeout=1.0)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


# --- reuse worker if it already has this user loaded (survives manager restarts) ---
def find_existing_worker_for_user(user_id: str) -> Optional[int]:
    with STATE_LOCK:
        ports = list(workers.keys())

    for port in ports:
        try:
            m = worker_metrics(port)
            if m and m.get("session_user") == user_id:
                return port
        except Exception:
            continue
    return None


def worker_status(port: int) -> Optional[dict]:
    url = f"http://{WORKER_HOST}:{port}/mt5/status"
    try:
        r = requests.get(url, timeout=2.0)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


def wait_for_worker_ready(port: int, attempts: int = 30, delay: float = 0.5) -> bool:
    for _ in range(attempts):
        if worker_metrics(port) is not None:
            return True
        time.sleep(delay)
    return False


def start_worker_process(port: int):
    if worker_metrics(port) is not None:
        return None

    os.makedirs(INSTANCES_BASE, exist_ok=True)
    os.makedirs(BROKERS_DIR, exist_ok=True)

    env = os.environ.copy()
    env["MT5_INSTANCES_ROOT"] = INSTANCES_BASE
    env["MT5_BROKERS_DIR"] = BROKERS_DIR
    env["MT5_BASE_DIR"] = r"C:\MT5\mt5-template"
    env["MT5_WORKER_PORT"] = str(port)

    worker_python = r"C:\MT5\worker_venv\Scripts\python.exe"
    cmd = [
        worker_python, "-m", "uvicorn", "worker_main:app",
        "--host", WORKER_HOST, "--port", str(port),
        "--log-level", "info",
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
    ok_count = 0
    for port in PRESTART_PORTS:
        try:
            proc = start_worker_process(port)
            with STATE_LOCK:
                workers[port] = {"proc": proc, "current_user": None, "last_used": 0.0}
            ok_count += 1
        except Exception as e:
            print(f"[prestart_workers] failed to start worker on port {port}: {e}")

    if ok_count == 0:
        raise RuntimeError("No workers could be started; check ports 8000-8019 are free.")


def start_dynamic_worker() -> int:
    with STATE_LOCK:
        used_ports = sorted(workers.keys()) if workers else []
        port = (max(used_ports) + 1) if used_ports else DYNAMIC_BASE_PORT

    proc = start_worker_process(port)
    with STATE_LOCK:
        workers[port] = {"proc": proc, "current_user": None, "last_used": 0.0}
    return port


def pick_and_reserve_worker(user_id: str) -> int:
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
        raise HTTPException(
            409,
            {"error": "worker_busy", "message": f"worker {port} is busy with {owner}", "owner": owner, "port": port},
        )


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
            resp = requests.post(url, json=payload, timeout=(5, 180))
            break
        except (ReqConnectionError, ReadTimeout):
            time.sleep(1.0)
    else:
        drop_user(user_id)
        raise HTTPException(502, {"error": "worker_unreachable", "message": f"worker on port {port} did not respond", "port": port})

    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    if resp.status_code != 200:
        drop_user(user_id)
        raise HTTPException(resp.status_code, {"error": "worker_login_failed", "worker_port": port, "detail": data})

    return data


def call_worker_cleanup_user(port: int, user_id: str):
    url = f"http://{WORKER_HOST}:{port}/cleanup-instance"
    try:
        requests.post(url, json={"user_id": user_id}, timeout=8)
    except Exception:
        pass


def call_worker_logout(port: int, user_id: str):
    url = f"http://{WORKER_HOST}:{port}/mt5/logout"
    try:
        requests.post(url, json={"user_id": user_id}, timeout=20)
    except Exception:
        pass


def touch_user(user_id: str):
    now = time.time()
    with STATE_LOCK:
        if user_id in user_sessions:
            user_sessions[user_id]["last_seen"] = now
            port = user_sessions[user_id]["port"]
            if port in workers:
                workers[port]["last_used"] = now


def fetch_symbols_for_user(port: int, user_id: str, fallback_server: Optional[str]) -> None:
    real_server = fallback_server

    try:
        r = requests.get(f"http://{WORKER_HOST}:{port}/mt5/account-info", timeout=6)
        if r.status_code == 200:
            data = r.json()
            real_server = data.get("server") or fallback_server
    except Exception:
        pass

    symbols: List[str] = []
    try:
        r = requests.get(f"http://{WORKER_HOST}:{port}/mt5/list-symbols", timeout=15)
        if r.status_code == 200:
            data = r.json()
            symbols = [s["name"] for s in data.get("symbols", [])]
    except Exception:
        pass

    if symbols:
        now = time.time()
        SYMBOL_CACHE[user_id] = {"server": real_server, "symbols": symbols, "fetched_at": now}
        save_symbol_cache()

    with STATE_LOCK:
        if user_id in user_sessions and real_server:
            user_sessions[user_id]["server"] = real_server


def get_symbols_for_user(user_id: str, fresh_seconds: int = 900) -> List[str]:
    now = time.time()
    cache = SYMBOL_CACHE.get(user_id)
    if cache and (now - cache.get("fetched_at", 0)) <= fresh_seconds:
        return list(cache.get("symbols") or [])

    with STATE_LOCK:
        sess = user_sessions.get(user_id)
    if not sess:
        return list(cache.get("symbols") or []) if cache else []

    port = sess["port"]
    try:
        r = requests.get(f"http://{WORKER_HOST}:{port}/mt5/list-symbols", timeout=15)
        if r.status_code == 200:
            data = r.json()
            syms = [s["name"] for s in data.get("symbols", [])]
            if syms:
                SYMBOL_CACHE[user_id] = {"server": sess.get("server"), "symbols": syms, "fetched_at": now}
                save_symbol_cache()
                return syms
    except Exception:
        pass

    return list(cache.get("symbols") or []) if cache else []


def resolve_symbol_from_cache(user_id: str, requested: str) -> str:
    req = requested.strip()
    if not req:
        return requested
    syms = get_symbols_for_user(user_id)
    if not syms:
        return requested

    req_low = req.lower()
    for s in syms:
        if s.lower() == req_low:
            return s

    candidates = [s for s in syms if s.lower().startswith(req_low)]
    if candidates:
        candidates.sort(key=len)
        return candidates[0]

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

    if user_id in SYMBOL_CACHE:
        SYMBOL_CACHE.pop(user_id, None)
        save_symbol_cache()


def cleanup_job():
    while True:
        time.sleep(REG_CLEAN_INTERVAL_SEC)
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

            if (now - last_seen) > IDLE_TTL_SEC:
                call_worker_cleanup_user(port, user_id)
                drop_user(user_id)


@app.on_event("startup")
def on_startup():
    os.makedirs(INSTANCES_BASE, exist_ok=True)
    os.makedirs(BROKERS_DIR, exist_ok=True)
    load_symbol_cache()

    prestart_workers()
    threading.Thread(target=cleanup_job, daemon=True).start()


@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": time.time()}


@app.get("/metrics")
def metrics():
    with STATE_LOCK:
        active_workers = {p: {"current_user": w["current_user"], "last_used": w["last_used"]} for p, w in workers.items()}
        active_sessions = len(user_sessions)

    enriched = {}
    for port, base in active_workers.items():
        st = worker_status(port)
        enriched[port] = {
            **base,
            "status": st or {"ok": False, "error": "status_unavailable"},
        }

    return {
        "manager": "running",
        "worker_count": len(active_workers),
        "workers": enriched,
        "active_sessions": active_sessions,
        "max_accounts": MAX_ACCOUNTS_PER_VPS,
        "idle_ttl_sec": IDLE_TTL_SEC,
        "timestamp": time.time(),
    }


@app.get("/symbols")
def list_symbols(user_id: str = Query(...), prefix: Optional[str] = Query(None)):
    touch_user(user_id)

    syms = get_symbols_for_user(user_id)
    if prefix:
        p = prefix.lower()
        syms = [s for s in syms if s.lower().startswith(p)]
    return {"ok": True, "count": len(syms), "symbols": syms}


@app.post("/connect")
def connect(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("userId") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "Missing user_id / userId / mt5_account_id in JSON body."})
    if "login" not in body or "password" not in body:
        raise HTTPException(400, {"error": "missing_creds", "message": "login and password are required"})

    lk = get_user_lock(user_id)
    with lk:
        login = int(body["login"])
        password = body["password"]
        server = body.get("server")
        timeout_ms = body.get("timeout_ms", 60000)

        with STATE_LOCK:
            sess = user_sessions.get(user_id)

        # If manager restarted but a worker still holds this user, reuse it.
        if not sess:
            existing_port = find_existing_worker_for_user(user_id)
            if existing_port is not None:
                now = time.time()
                with STATE_LOCK:
                    workers[existing_port]["current_user"] = user_id
                    workers[existing_port]["last_used"] = now
                    user_sessions[user_id] = {"port": existing_port, "last_seen": now, "server": server}
                sess = user_sessions[user_id]

        if sess:
            port = sess["port"]
            _assert_worker_owned_by(user_id, port)

            st = worker_status(port)
            if st and st.get("terminal_running") and st.get("mt5_logged_in"):
                touch_user(user_id)
                fetch_symbols_for_user(port, user_id, server)
                return {"ok": True, "reused": True, "worker_port": port, "note": "already connected"}

            result = call_worker_open(port, user_id, login, password, server, timeout_ms)
            touch_user(user_id)
            fetch_symbols_for_user(port, user_id, server)
            return {"ok": True, "reused": True, "worker_port": port, "result": result}

        with STATE_LOCK:
            current_active = len(user_sessions)
        if current_active >= MAX_ACCOUNTS_PER_VPS:
            raise HTTPException(503, {"error": "instance_full", "active_sessions": current_active, "max_accounts": MAX_ACCOUNTS_PER_VPS})

        port = pick_and_reserve_worker(user_id)
        now = time.time()
        with STATE_LOCK:
            user_sessions[user_id] = {"port": port, "last_seen": now, "server": server}

        try:
            result = call_worker_open(port, user_id, login, password, server, timeout_ms)
        except HTTPException as e:
            drop_user(user_id)
            raise e

        touch_user(user_id)
        fetch_symbols_for_user(port, user_id, server)
        return {"ok": True, "reused": False, "worker_port": port, "result": result}


@app.post("/logout")
def logout(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("userId") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "Missing user_id / userId / mt5_account_id in JSON body."})

    lk = get_user_lock(user_id)
    with lk:
        with STATE_LOCK:
            sess = user_sessions.get(user_id)

        if sess:
            port = sess["port"]
            call_worker_logout(port, user_id)
            drop_user(user_id)
            return {"ok": True, "user_id": user_id, "logged_out": True, "mode": "session_logout"}

        with STATE_LOCK:
            ports = list(workers.keys())

        for p in ports:
            call_worker_logout(p, user_id)

        SYMBOL_CACHE.pop(user_id, None)
        save_symbol_cache()

        return {"ok": True, "user_id": user_id, "logged_out": True, "mode": "broadcast_logout"}


def ensure_user_connected(
    user_id: str,
    login: Optional[int] = None,
    password: Optional[str] = None,
    server: Optional[str] = None,
    timeout_ms: Optional[int] = 60000
) -> int:
    lk = get_user_lock(user_id)
    with lk:
        with STATE_LOCK:
            sess = user_sessions.get(user_id)
        if sess:
            touch_user(user_id)
            return sess["port"]

        # Manager restarted? try to find already-loaded worker
        existing_port = find_existing_worker_for_user(user_id)
        if existing_port is not None:
            now = time.time()
            with STATE_LOCK:
                workers[existing_port]["current_user"] = user_id
                workers[existing_port]["last_used"] = now
                user_sessions[user_id] = {"port": existing_port, "last_seen": now, "server": server}
            touch_user(user_id)
            fetch_symbols_for_user(existing_port, user_id, server)
            return existing_port

        if login is None or password is None:
            raise HTTPException(400, {"error": "not_connected", "message": "user not connected and no login/password provided"})

        with STATE_LOCK:
            current_active = len(user_sessions)
        if current_active >= MAX_ACCOUNTS_PER_VPS:
            raise HTTPException(503, {"error": "instance_full", "active_sessions": current_active, "max_accounts": MAX_ACCOUNTS_PER_VPS})

        port = pick_and_reserve_worker(user_id)
        now = time.time()
        with STATE_LOCK:
            user_sessions[user_id] = {"port": port, "last_seen": now, "server": server}

        try:
            _ = call_worker_open(port, user_id, login, password, server, timeout_ms)
        except HTTPException as e:
            drop_user(user_id)
            raise e

        touch_user(user_id)
        fetch_symbols_for_user(port, user_id, server)
        return port


def call_worker_place_order(port: int, payload: dict) -> dict:
    url = f"http://{WORKER_HOST}:{port}/mt5/place-order"
    r = requests.post(url, json=payload, timeout=20)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if r.status_code != 200:
        raise HTTPException(r.status_code, {"error": "worker_trade_failed", "worker_port": port, "detail": data})
    return data


def call_worker_modify_sltp(port: int, payload: dict) -> dict:
    url = f"http://{WORKER_HOST}:{port}/mt5/modify-sltp"
    r = requests.post(url, json=payload, timeout=20)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if r.status_code != 200:
        raise HTTPException(r.status_code, {"error": "worker_modify_failed", "worker_port": port, "detail": data})
    return data


def call_worker_close_trade(port: int, payload: dict) -> dict:
    url = f"http://{WORKER_HOST}:{port}/mt5/close-position"
    r = requests.post(url, json=payload, timeout=25)
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if r.status_code != 200:
        raise HTTPException(r.status_code, {"error": "worker_close_failed", "worker_port": port, "detail": data})
    return data


@app.post("/place-trade")
def place_trade(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "user_id is required"})

    symbol_req = body.get("symbol")
    if not symbol_req:
        raise HTTPException(400, {"error": "missing_symbol", "message": "symbol is required"})

    login = body.get("login")
    password = body.get("password")
    server = body.get("server")
    timeout_ms = body.get("timeout_ms", 60000)

    port = ensure_user_connected(user_id, login=login, password=password, server=server, timeout_ms=timeout_ms)
    resolved = resolve_symbol_from_cache(user_id, symbol_req)

    worker_payload = {
        "user_id": user_id,
        "symbol": resolved,
        "volume": body.get("volume", 0.01),
        "order_type": body.get("order_type", "buy"),
        "sl": body.get("sl"),
        "tp": body.get("tp"),
        "price": body.get("price"),
        "stop_price": body.get("stop_price"),
        "deviation": body.get("deviation", 20),
        "comment": body.get("comment"),
        "magic": body.get("magic"),
    }

    result = call_worker_place_order(port, worker_payload)
    touch_user(user_id)

    return {"ok": True, "worker_port": port, "requested_symbol": symbol_req, "resolved_symbol": resolved, "result": result}


@app.post("/modify-sltp")
def modify_sltp(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "user_id is required"})

    login = body.get("login")
    password = body.get("password")
    server = body.get("server")
    timeout_ms = body.get("timeout_ms", 60000)

    port = ensure_user_connected(user_id, login=login, password=password, server=server, timeout_ms=timeout_ms)

    worker_payload = {
        "user_id": user_id,
        "ticket": body.get("ticket") or body.get("position") or body.get("position_ticket"),
        "sl": body.get("sl"),
        "tp": body.get("tp"),
    }

    result = call_worker_modify_sltp(port, worker_payload)
    touch_user(user_id)
    return {"ok": True, "worker_port": port, "result": result}


@app.post("/close-trade")
def close_trade(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "user_id is required"})

    login = body.get("login")
    password = body.get("password")
    server = body.get("server")
    timeout_ms = body.get("timeout_ms", 60000)

    port = ensure_user_connected(user_id, login=login, password=password, server=server, timeout_ms=timeout_ms)

    worker_payload = {
        "user_id": user_id,
        "ticket": body.get("ticket") or body.get("position") or body.get("position_ticket"),
        "volume": body.get("volume"),
        "deviation": body.get("deviation", 20),
        "comment": body.get("comment"),
        "magic": body.get("magic"),
    }

    result = call_worker_close_trade(port, worker_payload)
    touch_user(user_id)
    return {"ok": True, "worker_port": port, "result": result}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
