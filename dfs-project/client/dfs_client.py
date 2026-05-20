"""DFS Client - Cliente para Sistema de Archivos Distribuido"""

import os
import logging
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BLOCK_SIZE = int(os.getenv("BLOCK_SIZE", 1024))

# Cliente -> DataNode (localhost con puertos expuestos)
DATANODE_PORTS = {
    "dn1": 8001,
    "dn2": 8002,
    "dn3": 8003,
}

# DataNode -> Réplica (nombres de contenedor Docker)
REPLICA_HOSTS = {
    "dn1": "datanode1",
    "dn2": "datanode2",
    "dn3": "datanode3",
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dfs_client")


class DFSClient:
    def __init__(self, namenode_host: str = "localhost", namenode_port: int = 8000):
        self.namenode_url = f"http://{namenode_host}:{namenode_port}"
        self.session = requests.Session()
        self.username = None
        self._setup_session()

    def _setup_session(self):
        retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    def _request(self, method: str, endpoint: str, **kwargs):
        return self.session.request(method, f"{self.namenode_url}{endpoint}", **kwargs)

    def login(self, username: str, password: str) -> bool:
        resp = self._request("POST", "/login", json={"username": username, "password": password})
        if resp.status_code == 200:
            self.username = username
            logger.info(f"Login exitoso como {username}")
            return True
        raise Exception("Credenciales inválidas" if resp.status_code == 401 else f"Error {resp.status_code}")

    def _get_datanode_url(self, node_id: str) -> str:
        return f"http://localhost:{DATANODE_PORTS.get(node_id, 8001)}"

    def _get_replica_host(self, node_id: str) -> str:
        return REPLICA_HOSTS.get(node_id, "datanode1")

    def _get_replica_port(self, node_id: str) -> int:
        return 8001

    def upload(self, local_path: str, dfs_path: str) -> bool:
        from tqdm import tqdm

        if not os.path.isfile(local_path):
            raise Exception(f"Archivo {local_path} no existe")

        file_size = os.path.getsize(local_path)

        resp = self._request("POST", "/upload/begin", json={
            "filename": dfs_path,
            "size": file_size,
            "username": self.username
        })
        resp.raise_for_status()
        plan = resp.json()
        blocks = plan["blocks"]
        block_size = plan.get("block_size", BLOCK_SIZE)

        try:
            with open(local_path, 'rb') as f:
                with tqdm(total=len(blocks), desc="Subiendo bloques", unit="bloque") as pbar:
                    for block in blocks:
                        block_id = block["id"]
                        block_index = block["index"]
                        primary_node = block["primary"]
                        replica_node = block["replica"]

                        f.seek(block_index * block_size)
                        data = f.read(block_size)

                        url = f"{self._get_datanode_url(primary_node)}/block/{block_id}"
                        headers = {
                            "X-Replica-Host": self._get_replica_host(replica_node),
                            "X-Replica-Port": str(self._get_replica_port(replica_node))
                        }

                        r = requests.post(url, data=data, headers=headers, timeout=30)
                        r.raise_for_status()
                        pbar.update(1)

            self._request("POST", "/upload/confirm", json={"filename": dfs_path, "username": self.username}).raise_for_status()
            logger.info(f"Upload completado: {file_size} bytes en {len(blocks)} bloques")
            return True
        except Exception:
            try:
                self._request("POST", "/upload/abort", json={"filename": dfs_path, "username": self.username})
            except:
                pass
            raise

    def ls(self, dfs_path: str) -> List[str]:
        resp = self._request("GET", f"/ls/{dfs_path.lstrip('/')}")
        if resp.status_code == 404:
            raise Exception(f"Directorio {dfs_path} no encontrado")
        resp.raise_for_status()
        return resp.json().get("entries", [])

    def mkdir(self, dfs_path: str) -> bool:
        resp = self._request("POST", "/mkdir", json={"path": dfs_path, "username": self.username})
        if resp.status_code == 409:
            raise Exception(f"Directorio {dfs_path} ya existe")
        resp.raise_for_status()
        return True

    def rm(self, dfs_path: str) -> bool:
        resp = self._request("DELETE", f"/rm/{dfs_path.lstrip('/')}")
        if resp.status_code == 404:
            raise Exception(f"Archivo {dfs_path} no encontrado")
        resp.raise_for_status()
        return True

    def rmdir(self, dfs_path: str) -> bool:
        if self.ls(dfs_path):
            raise Exception(f"Directorio {dfs_path} no está vacío")
        return True