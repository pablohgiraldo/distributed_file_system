import logging
import os
import shutil
import socket
import threading
import time

import grpc
import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

import dfs_pb2
import dfs_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("datanode")

NODE_ID = os.getenv("NODE_ID", "dn-unknown")
NAMENODE_HOST = os.getenv("NAMENODE_HOST", "namenode")
NAMENODE_GRPC_PORT = int(os.getenv("NAMENODE_GRPC_PORT", 50051))
DATANODE_PORT = int(os.getenv("DATANODE_PORT", 8001))
BLOCK_SIZE = int(os.getenv("BLOCK_SIZE", 1024))
BLOCKS_DIR = "/blocks"

os.makedirs(BLOCKS_DIR, exist_ok=True)

app = FastAPI(title=f"DataNode {NODE_ID}")


# ---------------------------------------------------------------------------
# gRPC — Register
# ---------------------------------------------------------------------------

_MAX_REGISTER_ATTEMPTS = 12
_REGISTER_RETRY_SEC = 5


def register_with_namenode() -> None:
    """Call NameNode.Register, retrying up to 1 minute on failure."""
    target = f"{NAMENODE_HOST}:{NAMENODE_GRPC_PORT}"
    own_host = socket.gethostname()

    for attempt in range(1, _MAX_REGISTER_ATTEMPTS + 1):
        try:
            with grpc.insecure_channel(target) as channel:
                stub = dfs_pb2_grpc.NameNodeServiceStub(channel)
                resp = stub.Register(
                    dfs_pb2.RegisterRequest(
                        node_id=NODE_ID,
                        host=own_host,
                        port=DATANODE_PORT,
                    ),
                    timeout=5,
                )
            if resp.success:
                logger.info("registered with NameNode as %s", NODE_ID)
                return
            logger.warning("NameNode rejected registration (attempt %d)", attempt)
        except grpc.RpcError as exc:
            logger.warning(
                "register attempt %d/%d failed: %s — retrying in %ds",
                attempt, _MAX_REGISTER_ATTEMPTS, exc.details(), _REGISTER_RETRY_SEC,
            )
        time.sleep(_REGISTER_RETRY_SEC)

    logger.critical(
        "could not register with NameNode after %d attempts — exiting",
        _MAX_REGISTER_ATTEMPTS,
    )
    raise SystemExit(1)


register_with_namenode()


# ---------------------------------------------------------------------------
# gRPC — Heartbeat loop
# ---------------------------------------------------------------------------

_HEARTBEAT_INTERVAL_SEC = 10


def _heartbeat_loop() -> None:
    target = f"{NAMENODE_HOST}:{NAMENODE_GRPC_PORT}"
    while True:
        time.sleep(_HEARTBEAT_INTERVAL_SEC)
        try:
            free_bytes = shutil.disk_usage(BLOCKS_DIR).free
            block_ids = list_blocks()
            with grpc.insecure_channel(target) as channel:
                stub = dfs_pb2_grpc.NameNodeServiceStub(channel)
                stub.Heartbeat(
                    dfs_pb2.HeartbeatRequest(
                        node_id=NODE_ID,
                        free_bytes=free_bytes,
                        block_ids=block_ids,
                    ),
                    timeout=5,
                )
            logger.debug("heartbeat sent (%d blocks, %d free bytes)", len(block_ids), free_bytes)
        except grpc.RpcError as exc:
            logger.error("heartbeat failed: %s", exc.details())
        except Exception as exc:
            logger.error("heartbeat error: %s", exc)


threading.Thread(target=_heartbeat_loop, daemon=True, name="heartbeat").start()


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def _block_path(block_id: str) -> str:
    return os.path.join(BLOCKS_DIR, f"{block_id}.bin")


def save_block(block_id: str, data: bytes) -> None:
    with open(_block_path(block_id), "wb") as f:
        f.write(data)
    logger.info("saved block %s (%d bytes)", block_id, len(data))


def load_block(block_id: str) -> bytes:
    path = _block_path(block_id)
    if not os.path.exists(path):
        raise FileNotFoundError(block_id)
    with open(path, "rb") as f:
        data = f.read()
    if not data:
        raise ValueError(f"block {block_id} exists but is empty or corrupt")
    return data


def delete_block(block_id: str) -> None:
    path = _block_path(block_id)
    try:
        os.remove(path)
        logger.info("deleted block %s", block_id)
    except FileNotFoundError:
        pass


def list_blocks() -> list:
    return [
        f[:-4]
        for f in os.listdir(BLOCKS_DIR)
        if f.endswith(".bin")
    ]


@app.get("/health")
def health():
    return {"status": "ok", "node_id": NODE_ID}


# ---------------------------------------------------------------------------
# Block write endpoints
# ---------------------------------------------------------------------------

@app.post("/block/{block_id}", status_code=200)
async def write_block(block_id: str, request: Request):
    """Primary write: save block locally then forward to replica."""
    replica_host = request.headers.get("X-Replica-Host")
    replica_port = request.headers.get("X-Replica-Port")

    data = await request.body()
    save_block(block_id, data)

    if replica_host and replica_port:
        replica_url = f"http://{replica_host}:{replica_port}/replicate"
        try:
            resp = requests.post(
                replica_url,
                content=data,
                headers={"X-Block-Id": block_id},
                timeout=30,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.error(
                "replication failed for block %s to %s:%s — %s",
                block_id, replica_host, replica_port, exc,
            )
            raise HTTPException(
                status_code=500,
                detail=f"replication failed: {exc}",
            )

    return {"block_id": block_id, "size": len(data)}


@app.post("/replicate", status_code=200)
async def replicate_block(request: Request):
    """Replica write: save block locally only, no further replication."""
    block_id = request.headers.get("X-Block-Id")
    if not block_id:
        raise HTTPException(status_code=400, detail="X-Block-Id header required")

    data = await request.body()
    save_block(block_id, data)
    return {"block_id": block_id, "size": len(data)}


# ---------------------------------------------------------------------------
# Block read / delete endpoints
# ---------------------------------------------------------------------------

@app.get("/block/{block_id}")
def read_block(block_id: str):
    """Return raw block bytes; 404 if missing, 500 if empty/corrupt."""
    try:
        data = load_block(block_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"block {block_id} not found")
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return Response(content=data, media_type="application/octet-stream")


@app.delete("/block/{block_id}", status_code=200)
def remove_block(block_id: str):
    """Idempotent delete — 200 even if the block did not exist."""
    delete_block(block_id)
    return {"block_id": block_id, "deleted": True}
