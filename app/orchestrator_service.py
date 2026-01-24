# orchestrator_service.py
# ✅ Production-ready routing:
# - If manager assignment is on SAME INSTANCE as orchestrator: use localhost (no hairpin issues)
# - Else: use manager PRIVATE IP (VPC routing)
# - Public IP only as a last-resort fallback (optional)

import os
import time
import threading
import logging
from typing import Dict, Any, List, Optional

import boto3
import requests
from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ---------------------------
# Config
# ---------------------------
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")

MANAGER_TAG_KEY = os.getenv("MANAGER_TAG_KEY", "Role")
MANAGER_TAG_VALUE = os.getenv("MANAGER_TAG_VALUE", "MT5Manager")

LAUNCH_TEMPLATE_NAME = (
    os.getenv("MT5_LAUNCH_TEMPLATE_NAME")
    or os.getenv("LAUNCH_TEMPLATE_NAME")
    or "mt5-manager-launch-template"
)

MAX_USERS_PER_INSTANCE = int(os.getenv("MAX_USERS_PER_INSTANCE", "10"))
MIN_RUNNING_INSTANCES = int(os.getenv("MIN_RUNNING_INSTANCES", "1"))

MANAGER_PORT = int(os.getenv("MANAGER_PORT", "9000"))
POLL_INTERVAL_SEC = int(os.getenv("ORCH_POLL_INTERVAL_SEC", "30"))

MANAGER_CONNECT_TIMEOUT_SEC = int(os.getenv("MANAGER_CONNECT_TIMEOUT_SEC", "120"))
MANAGER_TRADE_TIMEOUT_SEC = int(os.getenv("MANAGER_TRADE_TIMEOUT_SEC", "30"))
MANAGER_MODIFY_TIMEOUT_SEC = int(os.getenv("MANAGER_MODIFY_TIMEOUT_SEC", "30"))
MANAGER_CLOSE_TIMEOUT_SEC = int(os.getenv("MANAGER_CLOSE_TIMEOUT_SEC", "30"))
MANAGER_LOGOUT_TIMEOUT_SEC = int(os.getenv("MANAGER_LOGOUT_TIMEOUT_SEC", "60"))

# If you want to FORCE localhost for same-instance calls
LOCAL_MANAGER_URL = os.getenv("LOCAL_MANAGER_URL", f"http://127.0.0.1:{MANAGER_PORT}")

# ✅ NEW: assignment TTL so we don't overcount after manager idle-disconnect
# Set this slightly above manager idle TTL (240s). Default 300s.
ASSIGNMENT_TTL_SEC = int(os.getenv("ORCH_ASSIGNMENT_TTL_SEC", "300"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("orchestrator")

ec2 = boto3.client("ec2", region_name=AWS_REGION)

app = FastAPI(
    title="MT5 Orchestrator",
    version="1.8.0",
    description="Routes user_id to the right MT5 manager and scales instances up. Uses localhost on same-instance, else private IP.",
)

STATE_LOCK = threading.Lock()
instances_state: Dict[str, Dict[str, Any]] = {}

# ✅ CHANGED: store last_seen to expire stale assignments
# ASSIGNMENTS[user_id] = {"instance_id": "...", "last_seen": 1234567890.0}
ASSIGNMENTS: Dict[str, Dict[str, Any]] = {}


# ---------------------------
# Error shaping
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


# ---------------------------
# Instance identity (for same-box localhost optimization)
# ---------------------------
def get_self_instance_id() -> Optional[str]:
    """
    Returns the EC2 instance-id if running on EC2 (IMDS).
    If not available, returns None.
    """
    try:
        r = requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            timeout=1,
        )
        if r.status_code == 200:
            return r.text.strip()
    except Exception:
        pass
    return None


SELF_INSTANCE_ID = get_self_instance_id()
if SELF_INSTANCE_ID:
    log.info("SELF_INSTANCE_ID detected: %s", SELF_INSTANCE_ID)
else:
    log.warning("SELF_INSTANCE_ID not detected (IMDS unavailable).")


# ---------------------------
# Helpers
# ---------------------------
def _now() -> float:
    return time.time()


def expire_stale_assignments(state: Dict[str, Dict[str, Any]]) -> None:
    """
    ✅ NEW: expire assignments that haven't been used for ASSIGNMENT_TTL_SEC.
    This prevents overcounting after manager auto-disconnect releases sessions.
    """
    now = _now()
    stale: List[str] = []
    for user_id, rec in ASSIGNMENTS.items():
        last_seen = float(rec.get("last_seen") or 0.0)
        if last_seen and (now - last_seen) > ASSIGNMENT_TTL_SEC:
            stale.append(user_id)

    for user_id in stale:
        ASSIGNMENTS.pop(user_id, None)

    if stale:
        log.info("Expired %d stale assignments (ttl=%ss)", len(stale), ASSIGNMENT_TTL_SEC)


def touch_assignment(user_id: str, instance_id: str):
    ASSIGNMENTS[user_id] = {"instance_id": instance_id, "last_seen": _now()}


def discover_manager_instances() -> List[Dict[str, Any]]:
    filters = [
        {"Name": f"tag:{MANAGER_TAG_KEY}", "Values": [MANAGER_TAG_VALUE]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ]
    instances: List[Dict[str, Any]] = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(Filters=filters):
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                instances.append(inst)
    return instances


def manager_connect_ip(entry: Dict[str, Any]) -> Optional[str]:
    """
    Always use private IP for inter-instance VPC routing.
    """
    priv = entry.get("private_ip")
    if priv:
        return priv
    return None


def manager_url_for(entry: Dict[str, Any]) -> Optional[str]:
    """
    If same instance -> localhost. Else -> private IP.
    """
    if SELF_INSTANCE_ID and entry.get("instance_id") == SELF_INSTANCE_ID:
        return LOCAL_MANAGER_URL

    ip = manager_connect_ip(entry)
    if not ip:
        return None
    return f"http://{ip}:{MANAGER_PORT}"


def fetch_manager_metrics(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = manager_url_for(entry)
    if not url:
        return None
    try:
        r = requests.get(f"{url}/metrics", timeout=3)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def count_active_users_from_metrics(metrics: Dict[str, Any]) -> int:
    # Prefer manager's own active_sessions if present
    if isinstance(metrics.get("active_sessions"), int):
        return int(metrics["active_sessions"])

    workers = metrics.get("workers", {})
    users = set()
    for w in workers.values():
        u = w.get("current_user")
        if u:
            users.add(u)
    return len(users)


def recompute_active_users_from_assignments(state: Dict[str, Dict[str, Any]]) -> None:
    for inst in state.values():
        inst["active_users"] = 0

    # count only non-stale assignments (they should be expired already, but safe)
    now = _now()
    for user_id, rec in ASSIGNMENTS.items():
        inst_id = rec.get("instance_id")
        last_seen = float(rec.get("last_seen") or 0.0)
        if not inst_id or inst_id not in state:
            continue
        if last_seen and (now - last_seen) > ASSIGNMENT_TTL_SEC:
            continue
        state[inst_id]["active_users"] += 1


def effective_users(inst: Dict[str, Any]) -> int:
    a = int(inst.get("active_users", 0) or 0)
    m = int(inst.get("metrics_active_users", 0) or 0)
    return max(a, m)


def sync_instances_state() -> None:
    global instances_state
    discovered = discover_manager_instances()
    now = _now()

    new_state: Dict[str, Dict[str, Any]] = {}

    with STATE_LOCK:
        expire_stale_assignments(instances_state)

    for inst in discovered:
        instance_id = inst["InstanceId"]
        state_name = inst.get("State", {}).get("Name", "unknown")

        private_ip = inst.get("PrivateIpAddress")
        public_ip = inst.get("PublicIpAddress")

        entry = {
            "instance_id": instance_id,
            "state": state_name,
            "private_ip": private_ip,
            "public_ip": public_ip,
            "active_users": 0,
            "metrics_active_users": 0,
            "last_metrics_ok": False,
            "last_checked": now,
        }

        if state_name == "running":
            metrics = fetch_manager_metrics(entry)
            if metrics:
                entry["last_metrics_ok"] = True
                entry["metrics_active_users"] = count_active_users_from_metrics(metrics)

        new_state[instance_id] = entry

    with STATE_LOCK:
        recompute_active_users_from_assignments(new_state)
        instances_state = new_state

    log.info("sync_instances_state: %d instances; assignments=%d", len(new_state), len(ASSIGNMENTS))


def get_stopped_instances() -> List[str]:
    filters = [
        {"Name": f"tag:{MANAGER_TAG_KEY}", "Values": [MANAGER_TAG_VALUE]},
        {"Name": "instance-state-name", "Values": ["stopped"]},
    ]
    out: List[str] = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(Filters=filters):
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                out.append(inst["InstanceId"])
    return out


def wait_for_instance_running(instance_id: str, timeout: int = 600) -> Dict[str, Any]:
    log.info("Waiting for instance %s to enter 'running' state...", instance_id)
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id], WaiterConfig={"Delay": 10, "MaxAttempts": timeout // 10})

    resp = ec2.describe_instances(InstanceIds=[instance_id])
    inst = resp["Reservations"][0]["Instances"][0]

    if not inst.get("PrivateIpAddress"):
        raise RuntimeError(f"Instance {instance_id} is running but has no PrivateIpAddress")
    return inst


def wait_for_manager_health(entry: Dict[str, Any], timeout: int = 300) -> None:
    url = manager_url_for(entry)
    if not url:
        raise RuntimeError("No manager URL available to check health.")

    health_url = f"{url}/health"
    log.info("Waiting for manager health: %s", health_url)

    deadline = _now() + timeout
    while _now() < deadline:
        try:
            r = requests.get(health_url, timeout=3)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(5.0)
    raise RuntimeError(f"Manager at {health_url} did not become healthy in {timeout} seconds")


def scale_out_new_or_stopped_instance() -> Dict[str, Any]:
    stopped = get_stopped_instances()
    if stopped:
        instance_id = stopped[0]
        log.info("Starting stopped instance %s", instance_id)
        ec2.start_instances(InstanceIds=[instance_id])
    else:
        log.info("Launching new instance from launch template %s", LAUNCH_TEMPLATE_NAME)
        resp = ec2.run_instances(
            LaunchTemplate={"LaunchTemplateName": LAUNCH_TEMPLATE_NAME},
            MinCount=1,
            MaxCount=1,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": MANAGER_TAG_KEY, "Value": MANAGER_TAG_VALUE}],
                }
            ],
        )
        instance_id = resp["Instances"][0]["InstanceId"]
        log.info("Launched new instance %s", instance_id)

    inst = wait_for_instance_running(instance_id)
    private_ip = inst.get("PrivateIpAddress")
    public_ip = inst.get("PublicIpAddress")

    entry = {
        "instance_id": instance_id,
        "state": "running",
        "private_ip": private_ip,
        "public_ip": public_ip,
        "active_users": 0,
        "metrics_active_users": 0,
        "last_metrics_ok": False,
        "last_checked": _now(),
    }

    wait_for_manager_health(entry)
    sync_instances_state()

    with STATE_LOCK:
        stored = instances_state.get(instance_id)

    return stored or entry


def poll_loop():
    while True:
        try:
            sync_instances_state()
        except Exception as e:
            log.exception("Error in orchestrator poll_loop: %s", e)
        time.sleep(POLL_INTERVAL_SEC)


@app.on_event("startup")
def on_startup():
    log.info("Orchestrator starting. Initial sync.")
    try:
        sync_instances_state()
    except Exception as e:
        log.exception("Initial sync failed (continuing): %s", e)

    threading.Thread(target=poll_loop, daemon=True).start()


# ---------------------------
# Models
# ---------------------------
class AssignRequest(BaseModel):
    user_id: str


class ReleaseRequest(BaseModel):
    user_id: str


class ConnectBody(BaseModel):
    user_id: str
    login: int
    password: str
    server: Optional[str] = None
    timeout_ms: Optional[int] = 60000


class LogoutRequest(BaseModel):
    user_id: str


# ---------------------------
# Routes
# ---------------------------
@app.get("/health")
def health():
    with STATE_LOCK:
        insts = list(instances_state.values())
        expire_stale_assignments(instances_state)
        running = [i for i in insts if i["state"] == "running"]
        return {
            "status": "ok",
            "region": AWS_REGION,
            "self_instance_id": SELF_INSTANCE_ID,
            "total_instances": len(insts),
            "running_instances": len(running),
            "max_users_per_instance": MAX_USERS_PER_INSTANCE,
            "assignment_ttl_sec": ASSIGNMENT_TTL_SEC,
            "assignments_count": len(ASSIGNMENTS),
            "assignments": ASSIGNMENTS,
        }


@app.get("/instances")
def list_instances():
    with STATE_LOCK:
        expire_stale_assignments(instances_state)
        recompute_active_users_from_assignments(instances_state)
        return {"instances": list(instances_state.values())}


@app.post("/assign")
def assign_instance(req: AssignRequest):
    user_id = req.user_id

    sync_instances_state()

    # Reuse assignment if still valid
    with STATE_LOCK:
        expire_stale_assignments(instances_state)
        existing = ASSIGNMENTS.get(user_id)
        if existing:
            existing_inst_id = existing.get("instance_id")
            if existing_inst_id and existing_inst_id in instances_state:
                inst = instances_state[existing_inst_id]
                url = manager_url_for(inst)
                if inst["state"] == "running" and url:
                    touch_assignment(user_id, inst["instance_id"])
                    recompute_active_users_from_assignments(instances_state)
                    return {
                        "ok": True,
                        "assigned_instance_id": inst["instance_id"],
                        "private_ip": inst.get("private_ip"),
                        "public_ip": inst.get("public_ip"),
                        "manager_url": url,
                        "note": "reused existing assignment",
                    }
            ASSIGNMENTS.pop(user_id, None)

    sync_instances_state()

    # Pick candidate
    with STATE_LOCK:
        expire_stale_assignments(instances_state)
        insts = list(instances_state.values())
        candidates = [
            i for i in insts
            if i["state"] == "running"
            and manager_url_for(i)
            and effective_users(i) < MAX_USERS_PER_INSTANCE
        ]
        if candidates:
            candidates.sort(key=lambda x: effective_users(x))
            chosen = candidates[0]
            touch_assignment(user_id, chosen["instance_id"])
            recompute_active_users_from_assignments(instances_state)
            url = manager_url_for(chosen)
            return {
                "ok": True,
                "assigned_instance_id": chosen["instance_id"],
                "private_ip": chosen.get("private_ip"),
                "public_ip": chosen.get("public_ip"),
                "manager_url": url,
                "note": "assigned existing running instance",
            }

    # Scale out
    try:
        new_inst = scale_out_new_or_stopped_instance()
    except Exception as e:
        raise HTTPException(500, {"error": "scale_out_failed", "message": str(e)})

    with STATE_LOCK:
        touch_assignment(user_id, new_inst["instance_id"])
        recompute_active_users_from_assignments(instances_state)

    url = manager_url_for(new_inst)
    return {
        "ok": True,
        "assigned_instance_id": new_inst["instance_id"],
        "private_ip": new_inst.get("private_ip"),
        "public_ip": new_inst.get("public_ip"),
        "manager_url": url,
        "note": "scaled out new or stopped instance",
    }


@app.post("/release")
def release_instance(req: ReleaseRequest):
    user_id = req.user_id
    with STATE_LOCK:
        inst_id = None
        rec = ASSIGNMENTS.pop(user_id, None)
        if rec:
            inst_id = rec.get("instance_id")
        recompute_active_users_from_assignments(instances_state)
    return {"ok": True, "released_instance_id": inst_id}


def _forward_json(manager_url: str, path: str, payload: dict, timeout_sec: int):
    url = f"{manager_url}{path}"
    try:
        r = requests.post(url, json=payload, timeout=timeout_sec)
    except Exception as e:
        log.exception("manager_unreachable url=%s", url)
        # ✅ If manager unreachable, drop assignment so next request can reassign
        user_id = payload.get("user_id") or payload.get("userId") or payload.get("mt5_account_id")
        if user_id:
            with STATE_LOCK:
                ASSIGNMENTS.pop(user_id, None)
        raise HTTPException(502, {"error": "manager_unreachable", "manager_url": manager_url, "detail": str(e)})

    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}

    if r.status_code == 200:
        return data

    # If manager says instance_full or other, we keep assignment (user likely still pinned).
    raise HTTPException(
        r.status_code,
        {"error": "manager_request_failed", "manager_url": manager_url, "path": path, "detail": data},
    )


@app.post("/connect")
def orchestrator_connect(body: ConnectBody):
    assign_resp = assign_instance(AssignRequest(user_id=body.user_id))
    manager_url = assign_resp["manager_url"]

    payload = {
        "user_id": body.user_id,
        "login": body.login,
        "password": body.password,
        "server": body.server,
        "timeout_ms": body.timeout_ms or 60000,
    }

    connect_result = _forward_json(manager_url, "/connect", payload, MANAGER_CONNECT_TIMEOUT_SEC)

    # ✅ touch activity so assignment doesn't expire while user is active
    with STATE_LOCK:
        touch_assignment(body.user_id, assign_resp["assigned_instance_id"])

    return {
        "ok": True,
        "manager_instance_id": assign_resp["assigned_instance_id"],
        "manager_url": manager_url,
        "connect_result": connect_result,
    }


@app.post("/place-trade")
def orchestrator_place_trade(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("userId") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "Missing user_id / userId / mt5_account_id in JSON body."})

    assign_resp = assign_instance(AssignRequest(user_id=user_id))
    manager_url = assign_resp["manager_url"]

    trade_result = _forward_json(manager_url, "/place-trade", body, MANAGER_TRADE_TIMEOUT_SEC)

    with STATE_LOCK:
        touch_assignment(user_id, assign_resp["assigned_instance_id"])

    return {
        "ok": True,
        "manager_instance_id": assign_resp["assigned_instance_id"],
        "manager_url": manager_url,
        "trade_result": trade_result,
    }


@app.post("/modify-sltp")
def orchestrator_modify_sltp(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("userId") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "Missing user_id / userId / mt5_account_id in JSON body."})

    assign_resp = assign_instance(AssignRequest(user_id=user_id))
    manager_url = assign_resp["manager_url"]

    result = _forward_json(manager_url, "/modify-sltp", body, MANAGER_MODIFY_TIMEOUT_SEC)

    with STATE_LOCK:
        touch_assignment(user_id, assign_resp["assigned_instance_id"])

    return {
        "ok": True,
        "manager_instance_id": assign_resp["assigned_instance_id"],
        "manager_url": manager_url,
        "result": result,
    }


@app.post("/close-trade")
def orchestrator_close_trade(body: dict = Body(...)):
    user_id = body.get("user_id") or body.get("userId") or body.get("mt5_account_id")
    if not user_id:
        raise HTTPException(400, {"error": "missing_user_id", "message": "Missing user_id / userId / mt5_account_id in JSON body."})

    assign_resp = assign_instance(AssignRequest(user_id=user_id))
    manager_url = assign_resp["manager_url"]

    result = _forward_json(manager_url, "/close-trade", body, MANAGER_CLOSE_TIMEOUT_SEC)

    with STATE_LOCK:
        touch_assignment(user_id, assign_resp["assigned_instance_id"])

    return {
        "ok": True,
        "manager_instance_id": assign_resp["assigned_instance_id"],
        "manager_url": manager_url,
        "result": result,
    }


@app.post("/logout")
def orchestrator_logout(req: LogoutRequest):
    user_id = req.user_id

    with STATE_LOCK:
        rec = ASSIGNMENTS.get(user_id)
        inst_id = rec.get("instance_id") if rec else None

    if inst_id:
        sync_instances_state()
        with STATE_LOCK:
            inst = instances_state.get(inst_id)

        if inst and inst.get("state") == "running":
            url = manager_url_for(inst)
            if url:
                try:
                    _ = _forward_json(url, "/logout", {"user_id": user_id}, MANAGER_LOGOUT_TIMEOUT_SEC)
                except Exception:
                    pass

    with STATE_LOCK:
        ASSIGNMENTS.pop(user_id, None)

    return {"ok": True, "user_id": user_id, "released": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator_service:app", host="0.0.0.0", port=9100)
