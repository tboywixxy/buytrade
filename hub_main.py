# hub_main.py
import os
import time
import threading
from typing import Dict, Any, Optional, List

import requests
from fastapi import FastAPI, HTTPException, Body

app = FastAPI(title="MT5 Hub", version="1.0.0")

# Comma-separated list of manager base URLs, e.g.:
# MT5_MANAGERS="http://10.0.1.10:9000,http://10.0.1.11:9000"
MANAGERS: List[str] = [
    m.strip()
    for m in os.environ.get("MT5_MANAGERS", "http://127.0.0.1:9000").split(",")
    if m.strip()
]

if not MANAGERS:
    raise RuntimeError("MT5_MANAGERS env must contain at least one manager base URL")

STATE_LOCK = threading.Lock()

# user_id -> manager_base_url
USER_TO_MANAGER: Dict[str, str] = {}

# manager_base_url -> stats
MANAGER_STATS: Dict[str, Dict[str, Any]] = {m: {} for m in MANAGERS}


def _get_manager_load_snapshot() -> Dict[str, int]:
    """
    Current #users assigned to each manager (based on USER_TO_MANAGER map).
    This is a simple 'load' metric.
    """
    counts = {m: 0 for m in MANAGERS}
    for _user, mgr in USER_TO_MANAGER.items():
        if mgr in counts:
            counts[mgr] += 1
    return counts


def pick_manager_for_user(user_id: str) -> str:
    """
    - If user already assigned, reuse same manager.
    - Else choose manager with smallest current user count.
    """
    with STATE_LOCK:
        existing = USER_TO_MANAGER.get(user_id)
        if existing:
            return existing

        load_counts = _get_manager_load_snapshot()
        # Choose manager with minimal load (ties broken by index order)
        best_mgr = min(MANAGERS, key=lambda m: load_counts.get(m, 0))

        USER_TO_MANAGER[user_id] = best_mgr
        return best_mgr


def refresh_manager_metrics_loop():
    """
    Background thread: periodically pull /metrics from each manager.
    This is optional but gives you visibility.
    """
    while True:
        for base in MANAGERS:
            try:
                r = requests.get(f"{base}/metrics", timeout=2)
                if r.status_code == 200:
                    data = r.json()
                    with STATE_LOCK:
                        MANAGER_STATS[base] = {
                            "last_seen": time.time(),
                            "data": data,
                        }
                else:
                    with STATE_LOCK:
                        MANAGER_STATS[base] = {
                            "last_seen": time.time(),
                            "error": f"status {r.status_code}",
                        }
            except Exception as e:
                with STATE_LOCK:
                    MANAGER_STATS[base] = {
                        "last_seen": time.time(),
                        "error": str(e),
                    }
        time.sleep(5)


@app.on_event("startup")
def on_startup():
    t = threading.Thread(target=refresh_manager_metrics_loop, daemon=True)
    t.start()


# ===== BASIC HUB ENDPOINTS =====

@app.get("/")
def root():
    return {
        "message": "MT5 Hub API",
        "status": "running",
        "version": "1.0.0",
        "managers": MANAGERS,
    }


@app.get("/health")
def health():
    with STATE_LOCK:
        # how many users mapped, how many managers known
        user_count = len(USER_TO_MANAGER)
        stats_copy = dict(MANAGER_STATS)
    return {
        "status": "healthy",
        "user_count": user_count,
        "manager_count": len(MANAGERS),
        "manager_stats": stats_copy,
        "timestamp": time.time(),
    }


@app.get("/metrics")
def metrics():
    with STATE_LOCK:
        load_counts = _get_manager_load_snapshot()
        stats_copy = dict(MANAGER_STATS)
    return {
        "hub": "running",
        "manager_load": load_counts,
        "manager_stats": stats_copy,
        "user_to_manager_count": len(USER_TO_MANAGER),
        "timestamp": time.time(),
    }


# ===== PROXY ENDPOINTS =====
# We expose the SAME contract as a manager: /connect and /place-trade,
# but we route to the right manager internally.


def _proxy_post(manager_base: str, path: str, body: dict, timeout: int = 20) -> Any:
    url = f"{manager_base}{path}"
    try:
        r = requests.post(url, json=body, timeout=timeout)
    except requests.exceptions.RequestException as e:
        raise HTTPException(502, f"Error contacting manager {manager_base}: {e}")

    # If manager errors, bubble up with same status code
    if r.status_code != 200:
        # try to preserve JSON error if present
        ct = r.headers.get("content-type", "")
        if ct.startswith("application/json"):
            raise HTTPException(r.status_code, r.json())
        else:
            raise HTTPException(r.status_code, r.text)

    try:
        return r.json()
    except ValueError:
        return {"raw": r.text}


@app.post("/connect")
def hub_connect(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("userId") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, "Missing user_id / userId / mt5_account_id in JSON body.")

    manager_base = pick_manager_for_user(user_id)
    data = _proxy_post(manager_base, "/connect", body, timeout=60)

    # Attach which manager handled this
    if isinstance(data, dict):
        data.setdefault("manager_base_url", manager_base)
    return data


@app.post("/place-trade")
def hub_place_trade(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, "user_id is required")

    # If user was never seen before at hub-level, we need to
    # pick a manager AND call its /connect first, because the manager
    # expects user to have an MT5 session.
    manager_base = pick_manager_for_user(user_id)

    # Optionally auto-connect if not already connected
    # (we rely on manager's ensure_user_connected to enforce login if needed)
    data = _proxy_post(manager_base, "/place-trade", body, timeout=30)

    if isinstance(data, dict):
        data.setdefault("manager_base_url", manager_base)
    return data


# ---- Run directly (dev) ----
if __name__ == "__main__":
    import uvicorn

    # Hub itself can be public-facing (e.g. ALB → hub)
    uvicorn.run(app, host="0.0.0.0", port=9100)
