import logging
import os

from fastapi import FastAPI

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


@app.get("/health")
def health():
    return {"status": "ok", "node_id": NODE_ID}
