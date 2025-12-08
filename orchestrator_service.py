# orchestrator_service.py
import os
import time
import threading
import logging
from typing import Dict, Any, List, Optional

import boto3
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ===== CONFIG =====
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")

# Tag used to identify MT5 manager instances
MANAGER_TAG_KEY = os.getenv("MANAGER_TAG_KEY", "Role")
MANAGER_TAG_VALUE = os.getenv("MANAGER_TAG_VALUE", "MT5Manager")

# Launch template for creating new manager instances
LAUNCH_TEMPLATE_NAME = (
    os.getenv("MT5_LAUNCH_TEMPLATE_NAME")
    or os.getenv("LAUNCH_TEMPLATE_NAME")
    or "mt5-manager-launch-template"
)

# How many distinct users per VPS (per manager instance)
MAX_USERS_PER_INSTANCE = int(os.getenv("MAX_USERS_PER_INSTANCE", "2"))

# Minimum number of running instances we want to keep
MIN_RUNNING_INSTANCES = int(os.getenv("MIN_RUNNING_INSTANCES", "1"))

# Manager API port
MANAGER_PORT = int(os.getenv("MANAGER_PORT", "9000"))

# How often the background loop refreshes metrics
POLL_INTERVAL_SEC = int(os.getenv("ORCH_POLL_INTERVAL_SEC", "30"))

# ==================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("orchestrator")

ec2 = boto3.client("ec2", region_name=AWS_REGION)

app = FastAPI(
    title="MT5 Orchestrator",
    version="1.3.0",
    description="Chooses which MT5 manager VPS each user should use and scales instances up.",
)

STATE_LOCK = threading.Lock()

# instances_state[instance_id] = {
#   "instance_id": str,
#   "public_ip": str or None,
#   "state": "running"/"stopped"/"...",
#   "active_users": int,          # derived from ASSIGNMENTS
#   "metrics_active_users": int,  # from manager /metrics
#   "last_metrics_ok": bool,
#   "last_checked": float,
# }
instances_state: Dict[str, Dict[str, Any]] = {}

# ASSIGNMENTS[user_id] = instance_id
ASSIGNMENTS: Dict[str, str] = {}


# ====== helpers to talk to EC2 & managers ======
def discover_manager_instances() -> List[Dict[str, Any]]:
    """
    Return all EC2 instances that have the MANAGER_TAG_KEY=MANAGER_TAG_VALUE tag,
    in any of these states: pending, running, stopping, stopped.
    """
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


def fetch_manager_metrics(ip: str) -> Optional[Dict[str, Any]]:
    url = f"http://{ip}:{MANAGER_PORT}/metrics"
    try:
        r = requests.get(url, timeout=3)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def count_active_users_from_metrics(metrics: Dict[str, Any]) -> int:
    """
    Our manager /metrics returns:
      {
        "manager": "running",
        "worker_count": 20,
        "workers": {
           "8000": {"current_user": "testuser1", "last_used": ...},
           ...
        }
      }

    We'll count UNIQUE non-null current_user values as "metrics_active_users".
    """
    workers = metrics.get("workers", {})
    users = set()
    for w in workers.values():
        u = w.get("current_user")
        if u:
            users.add(u)
    return len(users)


def recompute_active_users_from_assignments(state: Dict[str, Dict[str, Any]]) -> None:
    """
    Derive active_users per instance from ASSIGNMENTS.
    """
    # reset
    for inst in state.values():
        inst["active_users"] = 0

    # count assignments
    for user_id, inst_id in ASSIGNMENTS.items():
        if inst_id in state:
            state[inst_id]["active_users"] += 1


def effective_users(inst: Dict[str, Any]) -> int:
    """
    Effective load on an instance = max(booking_count, real_metrics_count).
    This prevents overfilling when tests hit managers directly.
    """
    a = int(inst.get("active_users", 0) or 0)
    m = int(inst.get("metrics_active_users", 0) or 0)
    return max(a, m)


def sync_instances_state() -> None:
    """
    Refresh instances_state from EC2 + manager /metrics,
    then overlay active_users from ASSIGNMENTS.
    """
    global instances_state

    discovered = discover_manager_instances()
    now = time.time()

    new_state: Dict[str, Dict[str, Any]] = {}

    for inst in discovered:
        instance_id = inst["InstanceId"]
        state_name = inst.get("State", {}).get("Name", "unknown")
        public_ip = inst.get("PublicIpAddress")

        entry = {
            "instance_id": instance_id,
            "state": state_name,
            "public_ip": public_ip,
            "active_users": 0,          # filled later from ASSIGNMENTS
            "metrics_active_users": 0,  # from /metrics
            "last_metrics_ok": False,
            "last_checked": now,
        }

        if state_name == "running" and public_ip:
            metrics = fetch_manager_metrics(public_ip)
            if metrics:
                entry["last_metrics_ok"] = True
                entry["metrics_active_users"] = count_active_users_from_metrics(metrics)

        new_state[instance_id] = entry

    # overlay active_users from ASSIGNMENTS
    recompute_active_users_from_assignments(new_state)

    with STATE_LOCK:
        instances_state = new_state

    log.info(
        "sync_instances_state: %d instances; assignments: %s",
        len(new_state),
        ASSIGNMENTS,
    )


def get_stopped_instances() -> List[str]:
    """
    Return IDs of stopped instances with our manager tag.
    """
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
    """
    Block until EC2 reports instance running and it has a PublicIpAddress.
    """
    log.info("Waiting for instance %s to enter 'running' state...", instance_id)
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id], WaiterConfig={"Delay": 10, "MaxAttempts": timeout // 10})

    # fetch full details
    resp = ec2.describe_instances(InstanceIds=[instance_id])
    inst = resp["Reservations"][0]["Instances"][0]

    # ensure it has public IP
    ip = inst.get("PublicIpAddress")
    if not ip:
        raise RuntimeError(f"Instance {instance_id} is running but has no PublicIpAddress")

    return inst


def wait_for_manager_health(ip: str, timeout: int = 300) -> None:
    """
    Poll /health until the manager responds OK or timeout.
    """
    url = f"http://{ip}:{MANAGER_PORT}/health"
    log.info("Waiting for manager at %s to be healthy...", url)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                log.info("Manager at %s is healthy", ip)
                return
        except Exception:
            pass
        time.sleep(5.0)
    raise RuntimeError(f"Manager at {ip} did not become healthy in {timeout} seconds")


def scale_out_new_or_stopped_instance() -> Dict[str, Any]:
    """
    Either start a stopped instance, or create a new one from the launch template.
    Returns a dict with instance_id and public_ip.
    """
    # 1) Check for stopped instances first
    stopped = get_stopped_instances()
    if stopped:
        instance_id = stopped[0]
        log.info("Starting stopped instance %s", instance_id)
        ec2.start_instances(InstanceIds=[instance_id])
    else:
        # 2) No stopped instances, create a brand new one
        log.info("No stopped instances; creating new from launch template %s", LAUNCH_TEMPLATE_NAME)
        resp = ec2.run_instances(
            LaunchTemplate={"LaunchTemplateName": LAUNCH_TEMPLATE_NAME},
            MinCount=1,
            MaxCount=1,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": MANAGER_TAG_KEY, "Value": MANAGER_TAG_VALUE},
                    ],
                }
            ],
        )
        instance_id = resp["Instances"][0]["InstanceId"]
        log.info("Launched new instance %s", instance_id)

    # 3) Wait until running + has IP
    inst = wait_for_instance_running(instance_id)
    public_ip = inst.get("PublicIpAddress")
    if not public_ip:
        raise RuntimeError(f"Instance {instance_id} has no PublicIpAddress after running")

    # 4) Wait for manager /health
    wait_for_manager_health(public_ip)

    # 5) Refresh global state and return
    sync_instances_state()

    with STATE_LOCK:
        entry = instances_state.get(instance_id)

    if not entry:
        entry = {
            "instance_id": instance_id,
            "public_ip": public_ip,
            "state": "running",
            "active_users": 0,
            "metrics_active_users": 0,
            "last_metrics_ok": True,
            "last_checked": time.time(),
        }

    return entry


def scale_in_if_idle() -> None:
    """
    Temporarily disabled: do not auto-stop instances yet.
    We'll manage idle cleanup manually for now.
    """
    return


# ===== background loop =====
def poll_loop():
    while True:
        try:
            sync_instances_state()
            scale_in_if_idle()
        except Exception as e:
            log.exception("Error in orchestrator poll_loop: %s", e)
        time.sleep(POLL_INTERVAL_SEC)


@app.on_event("startup")
def on_startup():
    log.info("Orchestrator starting up. Initial sync.")
    try:
        sync_instances_state()
    except Exception as e:
        log.exception("Initial sync failed, but continuing startup anyway: %s", e)

    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()



# ===== API models & endpoints =====
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


@app.get("/health")
def health():
    with STATE_LOCK:
        insts = list(instances_state.values())
    running = [i for i in insts if i["state"] == "running"]
    return {
        "status": "ok",
        "region": AWS_REGION,
        "total_instances": len(insts),
        "running_instances": len(running),
        "max_users_per_instance": MAX_USERS_PER_INSTANCE,
        "assignments": ASSIGNMENTS,
    }


@app.get("/instances")
def list_instances():
    # ensure counts reflect ASSIGNMENTS before returning
    with STATE_LOCK:
        recompute_active_users_from_assignments(instances_state)
        return {"instances": list(instances_state.values())}


@app.post("/assign")
def assign_instance(req: AssignRequest):
    user_id = req.user_id

    # If user already assigned, reuse that instance if it's still valid
    sync_instances_state()
    with STATE_LOCK:
        existing_inst_id = ASSIGNMENTS.get(user_id)
        if existing_inst_id and existing_inst_id in instances_state:
            inst = instances_state[existing_inst_id]
            ip = inst.get("public_ip")
            if inst["state"] == "running" and ip:
                recompute_active_users_from_assignments(instances_state)
                log.info("Reusing existing assignment for %s -> %s", user_id, existing_inst_id)
                return {
                    "ok": True,
                    "assigned_instance_id": inst["instance_id"],
                    "public_ip": ip,
                    "manager_url": f"http://{ip}:{MANAGER_PORT}",
                    "note": "reused existing assignment",
                }
        elif existing_inst_id:
            # instance vanished, drop stale mapping
            ASSIGNMENTS.pop(user_id, None)

    # Fresh assignment needed
    sync_instances_state()
    with STATE_LOCK:
        insts = list(instances_state.values())

        # 1) Try to find a running instance with capacity
        candidates = [
            i
            for i in insts
            if i["state"] == "running"
            and i.get("public_ip")
            and effective_users(i) < MAX_USERS_PER_INSTANCE
        ]

        if candidates:
            # fill least-loaded instance first, based on effective users
            candidates.sort(key=lambda x: effective_users(x))
            chosen = candidates[0]
            ASSIGNMENTS[user_id] = chosen["instance_id"]
            recompute_active_users_from_assignments(instances_state)
            ip = chosen["public_ip"]
            log.info(
                "Assigned %s to existing instance %s (effective_users=%d)",
                user_id,
                chosen["instance_id"],
                effective_users(chosen),
            )
            return {
                "ok": True,
                "assigned_instance_id": chosen["instance_id"],
                "public_ip": ip,
                "manager_url": f"http://{ip}:{MANAGER_PORT}",
                "note": "assigned existing running instance",
            }

    # 2) No capacity anywhere -> scale out (start stopped or launch new)
    try:
        new_inst = scale_out_new_or_stopped_instance()
    except Exception as e:
        raise HTTPException(500, f"Failed to scale out: {e}")

    with STATE_LOCK:
        ASSIGNMENTS[user_id] = new_inst["instance_id"]
        recompute_active_users_from_assignments(instances_state)

    ip = new_inst["public_ip"]
    log.info(
        "Assigned %s to NEW instance %s (effective_users=0)",
        user_id,
        new_inst["instance_id"],
    )
    return {
        "ok": True,
        "assigned_instance_id": new_inst["instance_id"],
        "public_ip": ip,
        "manager_url": f"http://{ip}:{MANAGER_PORT}",
        "note": "scaled out new or stopped instance",
    }


@app.post("/release")
def release_instance(req: ReleaseRequest):
    """
    Optional endpoint so your backend can tell the orchestrator
    that a user no longer needs their assigned instance.
    """
    user_id = req.user_id

    with STATE_LOCK:
        inst_id = ASSIGNMENTS.pop(user_id, None)
        recompute_active_users_from_assignments(instances_state)

    log.info("Released %s from %s", user_id, inst_id)
    return {
        "ok": True,
        "released_instance_id": inst_id,
    }


@app.post("/connect")
def orchestrator_connect(body: ConnectBody):
    """
    High-level connect:
    - choose / launch a manager instance for this user
    - forward the /connect call to that manager
    - return combined result
    """
    user_id = body.user_id

    # 1) Use the same logic as /assign to pick a manager
    assign_resp = assign_instance(AssignRequest(user_id=user_id))
    manager_url = assign_resp["manager_url"]  # e.g. http://52.xx.yy.zz:9000

    # 2) Build payload for the manager's /connect
    connect_url = f"{manager_url}/connect"
    payload = {
        "user_id": user_id,
        "login": body.login,
        "password": body.password,
        "server": body.server,
        "timeout_ms": body.timeout_ms or 60000,
    }

    # 3) Call manager
    try:
        r = requests.post(connect_url, json=payload, timeout=120)
    except Exception as e:
        raise HTTPException(
            502,
            {
                "error": "manager_unreachable",
                "manager_url": manager_url,
                "detail": str(e),
            },
        )

    # 4) Bubble up manager error nicely if not 200
    if r.status_code != 200:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise HTTPException(
            r.status_code,
            {
                "error": "manager_connect_failed",
                "manager_url": manager_url,
                "detail": detail,
            },
        )

    connect_result = r.json()

    # 5) Return combined view (so caller knows which VPS it landed on)
    return {
        "ok": True,
        "manager_instance_id": assign_resp["assigned_instance_id"],
        "manager_url": manager_url,
        "connect_result": connect_result,
    }
# ---- Run directly (service entrypoint) ----
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "orchestrator_service:app",
        host="0.0.0.0",
        port=9100,
    )

