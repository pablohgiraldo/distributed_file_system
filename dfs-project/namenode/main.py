"""DFS NameNode — FastAPI REST + gRPC + metadata persistence."""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import uuid
from concurrent import futures
from contextlib import asynccontextmanager
from typing import Any

import grpc
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import dfs_pb2
import dfs_pb2_grpc

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("namenode")

METADATA_PATH = os.getenv("METADATA_PATH", "/data/metadata.json")
BLOCK_SIZE = int(os.getenv("BLOCK_SIZE", 1024))
HEARTBEAT_TIMEOUT_SEC = int(os.getenv("HEARTBEAT_TIMEOUT_SEC", 30))
PENDING_TTL_SEC = int(os.getenv("PENDING_TTL_SEC", 300))
HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", 5))

GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))

_meta_lock = threading.Lock()
_metadata: dict[str, Any] = {}

DEFAULT_META = {
    "users": {},
    "files": {},
    "datanodes": {},
    "dirs": [],
}


def ensure_structure(meta: dict[str, Any]) -> None:
    for key, default in DEFAULT_META.items():
        if key not in meta:
            meta[key] = json.loads(json.dumps(default))


def load_metadata() -> None:
    """Load metadata from disk into ``_metadata`` (must hold ``_meta_lock``)."""
    global _metadata
    if os.path.exists(METADATA_PATH):
        with open(METADATA_PATH, encoding="utf-8") as f:
            _metadata = json.load(f)
        ensure_structure(_metadata)
    else:
        os.makedirs(os.path.dirname(METADATA_PATH) or ".", exist_ok=True)
        _metadata = {}
        ensure_structure(_metadata)
        save_metadata_unlocked()


def save_metadata_unlocked() -> None:
    """Persist ``_metadata`` (must hold ``_meta_lock``)."""
    ensure_structure(_metadata)
    directory = os.path.dirname(METADATA_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = METADATA_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(_metadata, f, indent=2)
    os.replace(tmp_path, METADATA_PATH)


def save_metadata() -> None:
    with _meta_lock:
        save_metadata_unlocked()


def alive_node_ids() -> list[str]:
    out: list[str] = []
    for nid, info in _metadata.get("datanodes", {}).items():
        if info.get("status") == "ALIVE":
            out.append(nid)
    out.sort()
    return out


def node_http_base(node_id: str) -> str | None:
    info = _metadata.get("datanodes", {}).get(node_id)
    if not info:
        return None
    host = info.get("host")
    port = info.get("port")
    if host is None or port is None:
        return None
    return f"http://{host}:{port}"


def delete_block_on_node_best_effort(node_id: str, block_id: str) -> None:
    base = node_http_base(node_id)
    if not base:
        log.warning("delete_block: unknown node %s", node_id)
        return
    url = f"{base}/block/{block_id}"
    try:
        r = requests.delete(url, timeout=HTTP_TIMEOUT_SEC)
        if r.status_code >= 400:
            log.warning("DELETE %s -> %s %s", url, r.status_code, r.text[:200])
    except requests.RequestException as e:
        log.warning("DELETE %s failed: %s", url, e)


def delete_blocks_best_effort(blocks: list[dict[str, Any]]) -> None:
    seen: set[tuple[str, str]] = set()
    for b in blocks:
        bid = b.get("id")
        if not bid:
            continue
        for role in ("primary", "replica"):
            nid = b.get(role)
            if not nid:
                continue
            key = (nid, bid)
            if key in seen:
                continue
            seen.add(key)
            delete_block_on_node_best_effort(nid, bid)


def copy_block_between_nodes(
    donor_id: str, target_id: str, block_id: str
) -> None:
    donor_base = node_http_base(donor_id)
    target_base = node_http_base(target_id)
    if not donor_base or not target_base:
        raise ValueError("unknown donor or target node")
    get_url = f"{donor_base}/block/{block_id}"
    post_url = f"{target_base}/block/{block_id}"
    r = requests.get(get_url, timeout=HTTP_TIMEOUT_SEC)
    r.raise_for_status()
    data = r.content
    r2 = requests.post(post_url, data=data, timeout=HTTP_TIMEOUT_SEC)
    r2.raise_for_status()


def repair_block_placement(filename: str, block_id: str) -> None:
    """Repair replication for one block if primary or replica is DEAD."""
    with _meta_lock:
        finfo = _metadata.get("files", {}).get(filename)
        if not finfo or finfo.get("status") != "READY":
            return
        block = next((b for b in finfo["blocks"] if b["id"] == block_id), None)
        if not block:
            return
        primary = block["primary"]
        replica = block["replica"]
        p_ok = _metadata["datanodes"].get(primary, {}).get("status") == "ALIVE"
        r_ok = _metadata["datanodes"].get(replica, {}).get("status") == "ALIVE"
        if p_ok and r_ok:
            return
        alive = alive_node_ids()
        if len(alive) < 2:
            log.warning(
                "repair_block_placement: need 2 alive nodes for %s block %s",
                filename,
                block_id,
            )
            return
        if not p_ok and not r_ok:
            log.warning(
                "repair_block_placement: both replicas dead for %s block %s",
                filename,
                block_id,
            )
            return
        if not p_ok:
            donor = replica
            new_primary = replica
            candidates = [n for n in alive if n != new_primary]
            if not candidates:
                return
            new_replica = candidates[0]
        else:
            donor = primary
            new_primary = primary
            candidates = [n for n in alive if n != new_primary]
            if not candidates:
                return
            new_replica = candidates[0]

    assert donor is not None and new_primary is not None and new_replica is not None
    try:
        copy_block_between_nodes(donor, new_replica, block_id)
    except Exception:
        log.exception(
            "re-replication failed file=%s block=%s donor=%s target=%s",
            filename,
            block_id,
            donor,
            new_replica,
        )
        return

    with _meta_lock:
        finfo = _metadata.get("files", {}).get(filename)
        if not finfo:
            return
        blk = next((b for b in finfo["blocks"] if b["id"] == block_id), None)
        if not blk:
            return
        blk["primary"] = new_primary
        blk["replica"] = new_replica
        save_metadata_unlocked()

    log.info(
        "re-replicated block %s file=%s primary=%s replica=%s",
        block_id,
        filename,
        new_primary,
        new_replica,
    )


def heartbeat_checker_loop() -> None:
    while True:
        time.sleep(HEARTBEAT_TIMEOUT_SEC)
        try:
            run_heartbeat_checker()
        except Exception:
            log.exception("heartbeat_checker")


def run_heartbeat_checker() -> None:
    now = int(time.time())
    with _meta_lock:
        changed = False
        for _nid, info in _metadata.get("datanodes", {}).items():
            last = int(info.get("last_seen", 0))
            if now - last > HEARTBEAT_TIMEOUT_SEC and info.get("status") == "ALIVE":
                info["status"] = "DEAD"
                changed = True
                log.warning("DataNode marked DEAD: last_seen=%s", last)
        if changed:
            save_metadata_unlocked()

    work: list[tuple[str, str]] = []
    with _meta_lock:
        for fname, finfo in _metadata.get("files", {}).items():
            if finfo.get("status") != "READY":
                continue
            for b in finfo.get("blocks", []):
                bid = b.get("id")
                if not bid:
                    continue
                p = b.get("primary")
                r = b.get("replica")
                p_ok = (
                    p
                    and _metadata["datanodes"].get(p, {}).get("status") == "ALIVE"
                )
                r_ok = (
                    r
                    and _metadata["datanodes"].get(r, {}).get("status") == "ALIVE"
                )
                if p_ok and r_ok:
                    continue
                if not p_ok and not r_ok:
                    continue
                work.append((fname, bid))

    for fname, bid in work:
        repair_block_placement(fname, bid)


def gc_pending_loop() -> None:
    while True:
        time.sleep(60)
        try:
            run_pending_gc()
        except Exception:
            log.exception("pending_gc")


def run_pending_gc() -> None:
    now = int(time.time())
    pending_files: list[tuple[str, dict[str, Any]]] = []
    with _meta_lock:
        for fname, finfo in list(_metadata.get("files", {}).items()):
            if finfo.get("status") != "PENDING":
                continue
            if now - int(finfo.get("created_at", 0)) > PENDING_TTL_SEC:
                pending_files.append((fname, dict(finfo)))

    for fname, finfo in pending_files:
        delete_blocks_best_effort(finfo.get("blocks", []))
        with _meta_lock:
            if _metadata.get("files", {}).get(fname, {}).get("status") == "PENDING":
                del _metadata["files"][fname]
                save_metadata_unlocked()
                log.info("GC removed stale PENDING file %s", fname)


# --- Pydantic bodies ---


class LoginBody(BaseModel):
    username: str
    password: str


class UploadBeginBody(BaseModel):
    filename: str
    size: int
    username: str


class UploadFilenameBody(BaseModel):
    filename: str
    username: str


class MkdirBody(BaseModel):
    path: str
    username: str


def normalize_file_path(path: str) -> str:
    p = path.strip()
    if not p.startswith("/"):
        p = "/" + p
    return p


def normalize_dir_path(path: str) -> str:
    p = path.strip()
    if not p.startswith("/"):
        p = "/" + p
    return p.rstrip("/") or "/"


def user_exists(username: str) -> bool:
    return username in _metadata.get("users", {})


def assert_user(username: str) -> None:
    if not user_exists(username):
        raise HTTPException(status_code=401, detail="Unknown user")


def new_block_id(existing: set[str]) -> str:
    for _ in range(20):
        bid = uuid.uuid4().hex[:6]
        if bid not in existing:
            return bid
    raise HTTPException(status_code=500, detail="Could not allocate block id")


def assign_blocks_round_robin(num_blocks: int) -> list[dict[str, Any]]:
    alive = alive_node_ids()
    if len(alive) < 2:
        raise HTTPException(
            status_code=503,
            detail="Need at least 2 ALIVE DataNodes for replication",
        )
    n = len(alive)
    blocks: list[dict[str, Any]] = []
    existing_ids: set[str] = set()
    for i in range(num_blocks):
        primary = alive[i % n]
        replica = alive[(i + 1) % n]
        bid = new_block_id(existing_ids)
        existing_ids.add(bid)
        blocks.append(
            {"id": bid, "index": i, "primary": primary, "replica": replica}
        )
    return blocks


# --- gRPC ---


class NameNodeGrpcServicer(dfs_pb2_grpc.NameNodeServiceServicer):
    def Register(self, request: dfs_pb2.RegisterRequest, context):  # type: ignore
        now = int(time.time())
        with _meta_lock:
            ensure_structure(_metadata)
            _metadata.setdefault("datanodes", {})[request.node_id] = {
                "host": request.host,
                "port": int(request.port),
                "last_seen": now,
                "status": "ALIVE",
                "free_bytes": 0,
                "block_ids": [],
            }
            save_metadata_unlocked()
        log.info(
            "Register DataNode %s %s:%s",
            request.node_id,
            request.host,
            request.port,
        )
        return dfs_pb2.RegisterResponse(success=True)

    def Heartbeat(self, request: dfs_pb2.HeartbeatRequest, context):  # type: ignore
        now = int(time.time())
        with _meta_lock:
            dn = _metadata.get("datanodes", {}).get(request.node_id)
            if not dn:
                log.warning("Heartbeat from unknown node %s", request.node_id)
                return dfs_pb2.HeartbeatResponse(ok=False)
            dn["last_seen"] = now
            dn["status"] = "ALIVE"
            dn["free_bytes"] = int(request.free_bytes)
            dn["block_ids"] = list(request.block_ids)
            save_metadata_unlocked()
        return dfs_pb2.HeartbeatResponse(ok=True)


def serve_grpc() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_NameNodeServiceServicer_to_server(NameNodeGrpcServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    log.info("gRPC listening on %s", GRPC_PORT)
    server.wait_for_termination()


_bg_started = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _bg_started
    with _meta_lock:
        load_metadata()
    if not _bg_started:
        _bg_started = True
        threading.Thread(target=serve_grpc, daemon=True).start()
        threading.Thread(target=heartbeat_checker_loop, daemon=True).start()
        threading.Thread(target=gc_pending_loop, daemon=True).start()
    yield


app = FastAPI(title="DFS NameNode", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/login")
def login(body: LoginBody):
    with _meta_lock:
        pw = _metadata.get("users", {}).get(body.username)
    if pw is None or pw != body.password:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return JSONResponse({"ok": True})


@app.post("/upload/begin")
def upload_begin(body: UploadBeginBody):
    with _meta_lock:
        assert_user(body.username)
        fname = normalize_file_path(body.filename)
        if fname in _metadata.get("files", {}):
            raise HTTPException(status_code=409, detail="File already exists")
        num_blocks = math.ceil(body.size / BLOCK_SIZE) if body.size > 0 else 0
        try:
            blocks = assign_blocks_round_robin(num_blocks)
        except HTTPException:
            raise
        created_at = int(time.time())
        _metadata.setdefault("files", {})[fname] = {
            "status": "PENDING",
            "size": body.size,
            "created_at": created_at,
            "blocks": blocks,
            "owner": body.username,
        }
        save_metadata_unlocked()
    return {"filename": fname, "blocks": blocks, "block_size": BLOCK_SIZE}


@app.post("/upload/confirm")
def upload_confirm(body: UploadFilenameBody):
    with _meta_lock:
        assert_user(body.username)
        fname = normalize_file_path(body.filename)
        finfo = _metadata.get("files", {}).get(fname)
        if not finfo:
            raise HTTPException(status_code=404, detail="File not found")
        if finfo.get("status") != "PENDING":
            raise HTTPException(status_code=400, detail="File is not PENDING")
        finfo["status"] = "READY"
        save_metadata_unlocked()
    return {"ok": True}


@app.post("/upload/abort")
def upload_abort(body: UploadFilenameBody):
    with _meta_lock:
        assert_user(body.username)
        fname = normalize_file_path(body.filename)
        finfo = _metadata.get("files", {}).get(fname)
        if not finfo:
            raise HTTPException(status_code=404, detail="File not found")
        if finfo.get("status") != "PENDING":
            raise HTTPException(status_code=400, detail="File is not PENDING")
        blocks = list(finfo.get("blocks", []))
    delete_blocks_best_effort(blocks)
    with _meta_lock:
        cur = _metadata.get("files", {}).get(fname)
        if cur and cur.get("status") == "PENDING":
            del _metadata["files"][fname]
            save_metadata_unlocked()
    return {"ok": True}


@app.get("/download/{filename:path}")
def download(filename: str):
    fname = normalize_file_path(filename)
    with _meta_lock:
        finfo = _metadata.get("files", {}).get(fname)
        if not finfo:
            raise HTTPException(status_code=404, detail="File not found")
        if finfo.get("status") != "READY":
            raise HTTPException(status_code=404, detail="File not ready")
        blocks = sorted(finfo["blocks"], key=lambda b: b["index"])
        out = [
            {
                "id": b["id"],
                "index": b["index"],
                "primary": b["primary"],
                "replica": b["replica"],
            }
            for b in blocks
        ]
        size = finfo.get("size", 0)
    return {"filename": fname, "size": size, "blocks": out}


@app.get("/ls/{path:path}")
def ls(path: str):
    prefix = normalize_dir_path(path)
    if prefix != "/":
        prefix_slash = prefix + "/"
    else:
        prefix_slash = "/"
    names: set[str] = set()
    with _meta_lock:
        dirs_list = list(_metadata.get("dirs", []))
        for fp, finfo in _metadata.get("files", {}).items():
            if finfo.get("status") != "READY":
                continue
            if prefix == "/":
                if not fp.startswith("/"):
                    continue
                remainder = fp.lstrip("/")
            else:
                if not (fp == prefix or fp.startswith(prefix_slash)):
                    continue
                remainder = fp[len(prefix_slash) :]
            if not remainder:
                continue
            if "/" in remainder:
                names.add(remainder.split("/", 1)[0] + "/")
            else:
                names.add(remainder)
        for d in dirs_list:
            dnorm = normalize_dir_path(d)
            if prefix == "/":
                if dnorm == "/":
                    continue
                rem = dnorm.lstrip("/")
            else:
                if not (dnorm == prefix or dnorm.startswith(prefix_slash)):
                    continue
                rem = dnorm[len(prefix_slash) :]
            if not rem:
                continue
            if "/" in rem:
                names.add(rem.split("/", 1)[0] + "/")
            else:
                names.add(rem + "/")
    return {"path": prefix, "entries": sorted(names)}


@app.delete("/rm/{filename:path}")
def rm(filename: str):
    fname = normalize_file_path(filename)
    with _meta_lock:
        finfo = _metadata.get("files", {}).get(fname)
        if not finfo:
            raise HTTPException(status_code=404, detail="File not found")
        blocks = list(finfo.get("blocks", []))
    delete_blocks_best_effort(blocks)
    with _meta_lock:
        if fname in _metadata.get("files", {}):
            del _metadata["files"][fname]
            save_metadata_unlocked()
    return {"ok": True}


@app.post("/mkdir")
def mkdir(body: MkdirBody):
    with _meta_lock:
        assert_user(body.username)
        dpath = normalize_dir_path(body.path)
        dirs = _metadata.setdefault("dirs", [])
        if dpath in dirs:
            raise HTTPException(status_code=409, detail="Directory exists")
        dirs.append(dpath)
        save_metadata_unlocked()
    return {"ok": True, "path": dpath}
