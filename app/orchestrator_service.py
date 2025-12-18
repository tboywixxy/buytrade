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

AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")

MANAGER_TAG_KEY = os.getenv("MANAGER_TAG_KEY", "Role")
MANAGER_TAG_VALUE = os.getenv("MANAGER_TAG_VALUE", "MT5Manager")

LAUNCH_TEMPLATE_NAME = (
    os.getenv("MT5_LAUNCH_TEMPLATE_NAME")
    or os.getenv("LAUNCH_TEMPLATE_NAME")
    or "mt5-manager-launch-template"
)

MAX_USERS_PER_INSTANCE = int(os.getenv("MAX_USERS_PER_INSTANCE", "50"))
MIN_RUNNING_INSTANCES = int(os.getenv("MIN_RUNNING_INSTANCES", "1"))

MANAGER_PORT = int(os.getenv("MANAGER_PORT", "9000"))
POLL_INTERVAL_SEC = int(os.getenv("ORCH_POLL_INTERVAL_SEC", "30"))

MANAGER_CONNECT_TIMEOUT_SEC = int(os.getenv("MANAGER_CONNECT_TIMEOUT_SEC", "120"))
MANAGER_TRADE_TIMEOUT_SEC = int(os.getenv("MANAGER_TRADE_TIMEOUT_SEC", "30"))
MANAGER_MODIFY_TIMEOUT_SEC = int(os.getenv("MANAGER_MODIFY_TIMEOUT_SEC", "30"))
MANAGER_CLOSE_TIMEOUT_SEC = int(os.getenv("MANAGER_CLOSE_TIMEOUT_SEC", "30"))
MANAGER_LOGOUT_TIMEOUT_SEC = int(os.getenv("MANAGER_LOGOUT_TIMEOUT_SEC", "60"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("orchestrator")

ec2 = boto3.client("ec2", region_name=AWS_REGION)

app = FastAPI(
    title="MT5 Orchestrator",
    version="1.6.0",
    description="Routes user_id to the right MT5 manager VPS and scales instances up.",
)

STATE_LOCK = threading.Lock()
instances_state: Dict[str, Dict[str, Any]] = {}
ASSIGNMENTS: Dict[str, str] = {}  # user_id -> instance_id


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
    for user_id, inst_id in ASSIGNMENTS.items():
        if inst_id in state:
            state[inst_id]["active_users"] += 1


def effective_users(inst: Dict[str, Any]) -> int:
    a = int(inst.get("active_users", 0) or 0)
    m = int(inst.get("metrics_active_users", 0) or 0)
    return max(a, m)


def sync_instances_state() -> None:
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
            "active_users": 0,
            "metrics_active_users": 0,
            "last_metrics_ok": False,
            "last_checked": now,
        }

        if state_name == "running" and public_ip:
            metrics = fetch_manager_metrics(public_ip)
            if metrics:
                entry["last_metrics_ok"] = True
                entry["metrics_active_users"] = count_active_users_from_metrics(metrics)

        new_state[instance_id] = entry

    recompute_active_users_from_assignments(new_state)

    with STATE_LOCK:
        instances_state = new_state

    log.info("sync_instances_state: %d instances; assignments=%s", len(new_state), ASSIGNMENTS)


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

    ip = inst.get("PublicIpAddress")
    if not ip:
        raise RuntimeError(f"Instance {instance_id} is running but has no PublicIpAddress")

    return inst


def wait_for_manager_health(ip: str, timeout: int = 300) -> None:
    url = f"http://{ip}:{MANAGER_PORT}/health"
    log.info("Waiting for manager health: %s", url)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(5.0)
    raise RuntimeError(f"Manager at {ip} did not become healthy in {timeout} seconds")


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
    public_ip = inst.get("PublicIpAddress")
    if not public_ip:
        raise RuntimeError(f"Instance {instance_id} has no PublicIpAddress after running")

    wait_for_manager_health(public_ip)
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
    with STATE_LOCK:
        recompute_active_users_from_assignments(instances_state)
        return {"instances": list(instances_state.values())}


@app.post("/assign")
def assign_instance(req: AssignRequest):
    user_id = req.user_id

    sync_instances_state()
    with STATE_LOCK:
        existing_inst_id = ASSIGNMENTS.get(user_id)
        if existing_inst_id and existing_inst_id in instances_state:
            inst = instances_state[existing_inst_id]
            ip = inst.get("public_ip")
            if inst["state"] == "running" and ip:
                recompute_active_users_from_assignments(instances_state)
                return {
                    "ok": True,
                    "assigned_instance_id": inst["instance_id"],
                    "public_ip": ip,
                    "manager_url": f"http://{ip}:{MANAGER_PORT}",
                    "note": "reused existing assignment",
                }
        elif existing_inst_id:
            ASSIGNMENTS.pop(user_id, None)

    sync_instances_state()
    with STATE_LOCK:
        insts = list(instances_state.values())
        candidates = [
            i for i in insts
            if i["state"] == "running"
            and i.get("public_ip")
            and effective_users(i) < MAX_USERS_PER_INSTANCE
        ]
        if candidates:
            candidates.sort(key=lambda x: effective_users(x))
            chosen = candidates[0]
            ASSIGNMENTS[user_id] = chosen["instance_id"]
            recompute_active_users_from_assignments(instances_state)
            ip = chosen["public_ip"]
            return {
                "ok": True,
                "assigned_instance_id": chosen["instance_id"],
                "public_ip": ip,
                "manager_url": f"http://{ip}:{MANAGER_PORT}",
                "note": "assigned existing running instance",
            }

    try:
        new_inst = scale_out_new_or_stopped_instance()
    except Exception as e:
        raise HTTPException(500, {"error": "scale_out_failed", "message": str(e)})

    with STATE_LOCK:
        ASSIGNMENTS[user_id] = new_inst["instance_id"]
        recompute_active_users_from_assignments(instances_state)

    ip = new_inst["public_ip"]
    return {
        "ok": True,
        "assigned_instance_id": new_inst["instance_id"],
        "public_ip": ip,
        "manager_url": f"http://{ip}:{MANAGER_PORT}",
        "note": "scaled out new or stopped instance",
    }


@app.post("/release")
def release_instance(req: ReleaseRequest):
    user_id = req.user_id
    with STATE_LOCK:
        inst_id = ASSIGNMENTS.pop(user_id, None)
        recompute_active_users_from_assignments(instances_state)
    return {"ok": True, "released_instance_id": inst_id}


def _forward_json(manager_url: str, path: str, payload: dict, timeout_sec: int):
    url = f"{manager_url}{path}"
    try:
        r = requests.post(url, json=payload, timeout=timeout_sec)
    except Exception as e:
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
        inst_id = ASSIGNMENTS.get(user_id)

    if inst_id:
        sync_instances_state()
        with STATE_LOCK:
            inst = instances_state.get(inst_id)

        if inst and inst.get("public_ip") and inst.get("state") == "running":
            manager_url = f"http://{inst['public_ip']}:{MANAGER_PORT}"
            try:
                _ = _forward_json(manager_url, "/logout", {"user_id": user_id}, MANAGER_LOGOUT_TIMEOUT_SEC)
            except Exception:
                pass

    with STATE_LOCK:
        ASSIGNMENTS.pop(user_id, None)

    return {"ok": True, "user_id": user_id, "released": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator_service:app", host="0.0.0.0", port=9100)
