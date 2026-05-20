import logging
import os

import requests
from fastapi import FastAPI, HTTPException, Request

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
