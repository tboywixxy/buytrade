# orchestrator_service.py
# ✅ Orchestrator-owned scaling (NO ASG):
# - Discovers managers by tags
# - Assigns users to least-loaded healthy manager
# - Scales OUT by starting stopped instances or launching new ones (Launch Template)
# - Scales IN by STOPPING idle managers (optionally TERMINATE extra stopped beyond pool limit)
# - Uses localhost when orchestrator + manager are on same EC2 instance
# - Uses manager PRIVATE IP for inter-instance traffic (VPC)
# - IMDSv2 supported for SELF_INSTANCE_ID

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
MANAGED_BY_TAG_KEY = os.getenv("MANAGED_BY_TAG_KEY", "ManagedBy")
MANAGED_BY_TAG_VALUE = os.getenv("MANAGED_BY_TAG_VALUE", "orchestrator")

# EC2 Launch Template used to create manager instances
LAUNCH_TEMPLATE_NAME = (
    os.getenv("MT5_LAUNCH_TEMPLATE_NAME")
    or os.getenv("LAUNCH_TEMPLATE_NAME")
    or "mt5-manager-launch-template"
)

# Capacity settings
MAX_USERS_PER_INSTANCE = int(os.getenv("MAX_USERS_PER_INSTANCE", "10"))
MIN_RUNNING_INSTANCES = int(os.getenv("MIN_RUNNING_INSTANCES", "1"))

# Scale-in behavior
SCALE_IN_IDLE_SEC = int(os.getenv("SCALE_IN_IDLE_SEC", "600"))  # stop idle managers after 10 mins
MAX_STOPPED_POOL = int(os.getenv("MAX_STOPPED_POOL", "2"))      # keep at most 2 stopped warm instances
ALLOW_TERMINATE_EXCESS_STOPPED = os.getenv("ALLOW_TERMINATE_EXCESS_STOPPED", "true").lower() == "true"

MANAGER_PORT = int(os.getenv("MANAGER_PORT", "9000"))
POLL_INTERVAL_SEC = int(os.getenv("ORCH_POLL_INTERVAL_SEC", "20"))

# request timeouts
MANAGER_CONNECT_TIMEOUT_SEC = int(os.getenv("MANAGER_CONNECT_TIMEOUT_SEC", "120"))
MANAGER_TRADE_TIMEOUT_SEC = int(os.getenv("MANAGER_TRADE_TIMEOUT_SEC", "30"))
MANAGER_MODIFY_TIMEOUT_SEC = int(os.getenv("MANAGER_MODIFY_TIMEOUT_SEC", "30"))
MANAGER_CLOSE_TIMEOUT_SEC = int(os.getenv("MANAGER_CLOSE_TIMEOUT_SEC", "30"))
MANAGER_LOGOUT_TIMEOUT_SEC = int(os.getenv("MANAGER_LOGOUT_TIMEOUT_SEC", "60"))

# If you want to FORCE localhost for same-instance calls
LOCAL_MANAGER_URL = os.getenv("LOCAL_MANAGER_URL", f"http://127.0.0.1:{MANAGER_PORT}")

# ✅ Assignment TTL: should be slightly ABOVE manager idle TTL (manager default 240s)
ASSIGNMENT_TTL_SEC = int(os.getenv("ORCH_ASSIGNMENT_TTL_SEC", "300"))

# Concurrency / safety
SCALE_MUTEX_TIMEOUT_SEC = int(os.getenv("SCALE_MUTEX_TIMEOUT_SEC", "180"))
WAIT_FOR_MANAGER_HEALTH_SEC = int(os.getenv("WAIT_FOR_MANAGER_HEALTH_SEC", "300"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("orchestrator")

ec2 = boto3.client("ec2", region_name=AWS_REGION)

app = FastAPI(
    title="MT5 Orchestrator (No ASG)",
    version="2.0.0",
    description="Routes user_id to MT5 manager instances and scales EC2 up/down WITHOUT Auto Scaling Groups.",
)

STATE_LOCK = threading.Lock()
SCALE_LOCK = threading.Lock()

instances_state: Dict[str, Dict[str, Any]] = {}

# ASSIGNMENTS[user_id] = {"instance_id": "...", "last_seen": float_ts}
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
# IMDSv2: self instance id
# ---------------------------
def _imds_get_token() -> Optional[str]:
    try:
        r = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1,
        )
        if r.status_code == 200:
            return r.text.strip()
    except Exception:
        pass
    return None


def get_self_instance_id() -> Optional[str]:
    """
    Returns the EC2 instance-id if running on EC2 (IMDSv2 preferred).
    If not available, returns None.
    """
    try:
        token = _imds_get_token()
        headers = {"X-aws-ec2-metadata-token": token} if token else {}
        r = requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers=headers,
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


def expire_stale_assignments() -> int:
    now = _now()
    stale: List[str] = []
    for user_id, rec in list(ASSIGNMENTS.items()):
        last_seen = float(rec.get("last_seen") or 0.0)
        if last_seen and (now - last_seen) > ASSIGNMENT_TTL_SEC:
            stale.append(user_id)

    for user_id in stale:
        ASSIGNMENTS.pop(user_id, None)

    if stale:
        log.info("Expired %d stale assignments (ttl=%ss)", len(stale), ASSIGNMENT_TTL_SEC)
    return len(stale)


def touch_assignment(user_id: str, instance_id: str):
    ASSIGNMENTS[user_id] = {"instance_id": instance_id, "last_seen": _now()}


def discover_manager_instances(states: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Discover managers by tags. If you also tag ManagedBy=orchestrator, it reduces accidental pickup.
    """
    if states is None:
        states = ["pending", "running", "stopping", "stopped"]

    filters = [
        {"Name": f"tag:{MANAGER_TAG_KEY}", "Values": [MANAGER_TAG_VALUE]},
        {"Name": f"tag:{MANAGED_BY_TAG_KEY}", "Values": [MANAGED_BY_TAG_VALUE]},
        {"Name": "instance-state-name", "Values": states},
    ]
    instances: List[Dict[str, Any]] = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(Filters=filters):
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                instances.append(inst)
    return instances


def manager_url_for(entry: Dict[str, Any]) -> Optional[str]:
    """
    If same instance -> localhost. Else -> private IP.
    """
    if SELF_INSTANCE_ID and entry.get("instance_id") == SELF_INSTANCE_ID:
        return LOCAL_MANAGER_URL

    priv = entry.get("private_ip")
    if not priv:
        return None
    return f"http://{priv}:{MANAGER_PORT}"


def fetch_manager_metrics(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = manager_url_for(entry)
    if not url:
        return None
    try:
        r = requests.get(f"{url}/metrics", timeout=2.5)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def count_active_users_from_metrics(metrics: Dict[str, Any]) -> int:
    # Prefer manager's own real active_sessions if present
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
        inst["assigned_users"] = 0

    now = _now()
    for _, rec in ASSIGNMENTS.items():
        inst_id = rec.get("instance_id")
        last_seen = float(rec.get("last_seen") or 0.0)
        if not inst_id or inst_id not in state:
            continue
        if last_seen and (now - last_seen) > ASSIGNMENT_TTL_SEC:
            continue
        state[inst_id]["assigned_users"] += 1


def effective_users(inst: Dict[str, Any]) -> int:
    """
    Use max(assigned_users, metrics_active_users).
    This avoids undercount when orchestrator restarts, and avoids overcount when manager drops sessions.
    """
    a = int(inst.get("assigned_users", 0) or 0)
    m = int(inst.get("metrics_active_users", 0) or 0)
    return max(a, m)


def wait_for_instance_running(instance_id: str, timeout: int = 600) -> Dict[str, Any]:
    log.info("Waiting for instance %s to enter 'running' state...", instance_id)
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id], WaiterConfig={"Delay": 10, "MaxAttempts": max(1, timeout // 10)})

    resp = ec2.describe_instances(InstanceIds=[instance_id])
    inst = resp["Reservations"][0]["Instances"][0]
    if not inst.get("PrivateIpAddress"):
        raise RuntimeError(f"Instance {instance_id} is running but has no PrivateIpAddress")
    return inst


def wait_for_manager_health(entry: Dict[str, Any], timeout: int = WAIT_FOR_MANAGER_HEALTH_SEC) -> None:
    url = manager_url_for(entry)
    if not url:
        raise RuntimeError("No manager URL available to check health.")

    health_url = f"{url}/health"
    log.info("Waiting for manager health: %s", health_url)

    deadline = _now() + timeout
    while _now() < deadline:
        try:
            r = requests.get(health_url, timeout=2.5)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(4.0)

    raise RuntimeError(f"Manager at {health_url} did not become healthy in {timeout} seconds")


def _merge_last_active(prev: Dict[str, Dict[str, Any]], new: Dict[str, Dict[str, Any]]) -> None:
    """
    Maintain per-instance last_active_ts.
    - If effective_users > 0 => update to now
    - Else keep old value
    """
    now = _now()
    for inst_id, entry in new.items():
        old = prev.get(inst_id, {})
        entry["last_active_ts"] = float(old.get("last_active_ts") or now)
        if entry.get("state") == "running" and effective_users(entry) > 0:
            entry["last_active_ts"] = now


def sync_instances_state() -> None:
    global instances_state
    discovered = discover_manager_instances()
    now = _now()

    new_state: Dict[str, Dict[str, Any]] = {}

    with STATE_LOCK:
        expire_stale_assignments()

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
            "assigned_users": 0,
            "metrics_active_users": 0,
            "last_metrics_ok": False,
            "last_checked": now,
            "last_active_ts": now,  # filled/merged later
        }

        if state_name == "running":
            metrics = fetch_manager_metrics(entry)
            if metrics:
                entry["last_metrics_ok"] = True
                entry["metrics_active_users"] = count_active_users_from_metrics(metrics)

        new_state[instance_id] = entry

    with STATE_LOCK:
        recompute_active_users_from_assignments(new_state)
        _merge_last_active(instances_state, new_state)
        instances_state = new_state

    log.info("sync_instances_state: %d instances; assignments=%d", len(new_state), len(ASSIGNMENTS))


def get_stopped_instances() -> List[str]:
    insts = discover_manager_instances(states=["stopped"])
    return [i["InstanceId"] for i in insts]


def get_pending_instances() -> List[str]:
    insts = discover_manager_instances(states=["pending"])
    return [i["InstanceId"] for i in insts]


def ensure_min_running_instances() -> None:
    """
    Ensure we have at least MIN_RUNNING_INSTANCES running.
    This is the only "baseline" behavior (no ASG).
    """
    with STATE_LOCK:
        running = [i for i in instances_state.values() if i.get("state") == "running"]

    if len(running) >= MIN_RUNNING_INSTANCES:
        return

    need = MIN_RUNNING_INSTANCES - len(running)
    log.info("Need %d more running instances to meet MIN_RUNNING_INSTANCES=%d", need, MIN_RUNNING_INSTANCES)

    for _ in range(need):
        _ = scale_out_new_or_stopped_instance()


def scale_out_new_or_stopped_instance() -> Dict[str, Any]:
    """
    Scale OUT:
    - if stopped exists => start it
    - elif pending exists => wait for it (don't launch another)
    - else => launch new from launch template
    """
    acquired = SCALE_LOCK.acquire(timeout=SCALE_MUTEX_TIMEOUT_SEC)
    if not acquired:
        raise RuntimeError("scale_lock_timeout")

    try:
        # If we already have pending, do NOT launch another: wait and resync.
        pending = get_pending_instances()
        if pending:
            log.info("Pending manager exists (%s). Waiting for it instead of launching new.", pending[0])
            inst = wait_for_instance_running(pending[0])
            entry = {
                "instance_id": pending[0],
                "state": "running",
                "private_ip": inst.get("PrivateIpAddress"),
                "public_ip": inst.get("PublicIpAddress"),
                "assigned_users": 0,
                "metrics_active_users": 0,
                "last_metrics_ok": False,
                "last_checked": _now(),
                "last_active_ts": _now(),
            }
            wait_for_manager_health(entry)
            sync_instances_state()
            with STATE_LOCK:
                stored = instances_state.get(pending[0])
            return stored or entry

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
                        "Tags": [
                            {"Key": MANAGER_TAG_KEY, "Value": MANAGER_TAG_VALUE},
                            {"Key": MANAGED_BY_TAG_KEY, "Value": MANAGED_BY_TAG_VALUE},
                        ],
                    }
                ],
            )
            instance_id = resp["Instances"][0]["InstanceId"]
            log.info("Launched new instance %s", instance_id)

        inst = wait_for_instance_running(instance_id)
        entry = {
            "instance_id": instance_id,
            "state": "running",
            "private_ip": inst.get("PrivateIpAddress"),
            "public_ip": inst.get("PublicIpAddress"),
            "assigned_users": 0,
            "metrics_active_users": 0,
            "last_metrics_ok": False,
            "last_checked": _now(),
            "last_active_ts": _now(),
        }

        wait_for_manager_health(entry)
        sync_instances_state()

        with STATE_LOCK:
            stored = instances_state.get(instance_id)

        return stored or entry
    finally:
        SCALE_LOCK.release()


def scale_in_stop_idle_instances() -> Dict[str, Any]:
    """
    Scale IN:
    - STOP one idle running instance if:
      - effective_users == 0
      - idle for SCALE_IN_IDLE_SEC
      - won't go below MIN_RUNNING_INSTANCES
    """
    with STATE_LOCK:
        insts = list(instances_state.values())
        running = [i for i in insts if i.get("state") == "running"]

    if len(running) <= MIN_RUNNING_INSTANCES:
        return {"action": "none", "reason": "at_min_running"}

    now = _now()
    idle_candidates = []
    for i in running:
        if effective_users(i) != 0:
            continue
        last_active = float(i.get("last_active_ts") or now)
        if (now - last_active) < SCALE_IN_IDLE_SEC:
            continue
        idle_candidates.append(i)

    if not idle_candidates:
        return {"action": "none", "reason": "no_idle_candidates"}

    idle_candidates.sort(key=lambda x: float(x.get("last_active_ts") or 0.0))
    victim = idle_candidates[0]["instance_id"]

    if (len(running) - 1) < MIN_RUNNING_INSTANCES:
        return {"action": "none", "reason": "would_go_below_min_running"}

    log.info("Scale-in: stopping idle instance %s", victim)
    try:
        ec2.stop_instances(InstanceIds=[victim])
    except Exception as e:
        return {"action": "error", "reason": "stop_failed", "detail": str(e), "victim": victim}

    return {"action": "stop", "victim": victim}


def enforce_stopped_pool_limit() -> Dict[str, Any]:
    """
    Keep at most MAX_STOPPED_POOL stopped instances.
    If exceeded, terminate oldest stopped instances (optional).
    """
    if not ALLOW_TERMINATE_EXCESS_STOPPED:
        return {"action": "none", "reason": "terminate_excess_disabled"}

    stopped_ids = get_stopped_instances()
    if len(stopped_ids) <= MAX_STOPPED_POOL:
        return {"action": "none", "reason": "within_pool_limit", "stopped": len(stopped_ids)}

    # describe to get LaunchTime
    resp = ec2.describe_instances(InstanceIds=stopped_ids)
    stopped_insts = []
    for r in resp.get("Reservations", []):
        for inst in r.get("Instances", []):
            stopped_insts.append(inst)

    stopped_insts.sort(key=lambda x: x.get("LaunchTime"))  # oldest first
    excess = max(0, len(stopped_insts) - MAX_STOPPED_POOL)
    victims = [x["InstanceId"] for x in stopped_insts[:excess]]

    terminated = []
    for vid in victims:
        try:
            log.info("Stopped pool exceeded: terminating stopped instance %s", vid)
            ec2.terminate_instances(InstanceIds=[vid])
            terminated.append(vid)
        except Exception:
            pass

    return {"action": "terminate", "terminated": terminated, "stopped_before": len(stopped_ids), "pool_limit": MAX_STOPPED_POOL}


def poll_loop():
    while True:
        try:
            sync_instances_state()
            ensure_min_running_instances()
            # Stop idle if possible
            _ = scale_in_stop_idle_instances()
            # Optional: keep stopped pool bounded
            _ = enforce_stopped_pool_limit()
            # refresh state after possible stop/terminate
            sync_instances_state()
        except Exception as e:
            log.exception("Error in orchestrator poll_loop: %s", e)
        time.sleep(POLL_INTERVAL_SEC)


@app.on_event("startup")
def on_startup():
    log.info("Orchestrator starting. Initial sync.")
    try:
        sync_instances_state()
        ensure_min_running_instances()
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
        expire_stale_assignments()
        insts = list(instances_state.values())
        running = [i for i in insts if i["state"] == "running"]
        stopped = [i for i in insts if i["state"] == "stopped"]
        pending = [i for i in insts if i["state"] == "pending"]

        return {
            "status": "ok",
            "region": AWS_REGION,
            "self_instance_id": SELF_INSTANCE_ID,
            "total_instances": len(insts),
            "running_instances": len(running),
            "stopped_instances": len(stopped),
            "pending_instances": len(pending),
            "max_users_per_instance": MAX_USERS_PER_INSTANCE,
            "min_running_instances": MIN_RUNNING_INSTANCES,
            "scale_in_idle_sec": SCALE_IN_IDLE_SEC,
            "assignment_ttl_sec": ASSIGNMENT_TTL_SEC,
            "assignments_count": len(ASSIGNMENTS),
            "assignments": ASSIGNMENTS,
        }


@app.get("/instances")
def list_instances():
    sync_instances_state()
    with STATE_LOCK:
        expire_stale_assignments()
        recompute_active_users_from_assignments(instances_state)
        # include computed effective_users for debugging
        out = []
        for inst in instances_state.values():
            x = dict(inst)
            x["effective_users"] = effective_users(inst)
            out.append(x)
        return {"instances": out}


@app.post("/assign")
def assign_instance(req: AssignRequest):
    user_id = req.user_id.strip()
    if not user_id:
        raise HTTPException(400, {"error": "invalid_user_id", "message": "user_id is empty"})

    sync_instances_state()

    # Reuse assignment if still valid and manager is healthy/running
    with STATE_LOCK:
        expire_stale_assignments()
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

    # Pick least-loaded running candidate
    with STATE_LOCK:
        expire_stale_assignments()
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

    # Need to scale out
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
        rec = ASSIGNMENTS.pop(user_id, None)
        inst_id = rec.get("instance_id") if rec else None
        recompute_active_users_from_assignments(instances_state)
    return {"ok": True, "released_instance_id": inst_id}


def _forward_json(manager_url: str, path: str, payload: dict, timeout_sec: int):
    url = f"{manager_url}{path}"
    try:
        r = requests.post(url, json=payload, timeout=timeout_sec)
    except Exception as e:
        log.exception("manager_unreachable url=%s", url)
        # If manager unreachable, drop assignment so next request can reassign
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

    assign_resp = assign_instance(AssignRequest(user_id=str(user_id)))
    manager_url = assign_resp["manager_url"]

    trade_result = _forward_json(manager_url, "/place-trade", body, MANAGER_TRADE_TIMEOUT_SEC)

    with STATE_LOCK:
        touch_assignment(str(user_id), assign_resp["assigned_instance_id"])

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

    assign_resp = assign_instance(AssignRequest(user_id=str(user_id)))
    manager_url = assign_resp["manager_url"]

    result = _forward_json(manager_url, "/modify-sltp", body, MANAGER_MODIFY_TIMEOUT_SEC)

    with STATE_LOCK:
        touch_assignment(str(user_id), assign_resp["assigned_instance_id"])

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

    assign_resp = assign_instance(AssignRequest(user_id=str(user_id)))
    manager_url = assign_resp["manager_url"]

    result = _forward_json(manager_url, "/close-trade", body, MANAGER_CLOSE_TIMEOUT_SEC)

    with STATE_LOCK:
        touch_assignment(str(user_id), assign_resp["assigned_instance_id"])

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
