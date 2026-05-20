import os 
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BLOCK_SIZE = int(os.getenv("BLOCK_SIZE", 1024))

DATANODE_MAP = {
    "dn1": ("localhost", 8001),
    "dn2": ("localhost", 8002),
    "dn3": ("localhost", 8003),
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dfs_client")


class DFSClient:

    def __init__(self, namenode_host: str = "localhost", namenode_port: int = 8000):
        self.namenode_url =  f"http://{namenode_host}:{namenode_port}"
        self.session = requests.Session()
        self.username = None
        self._setup_session()

    def _setup_session(self):
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.namenode_url}{endpoint}"
        response = self.session.request(method, url, **kwargs)
        return response
    
    def _get_datanode_url(self, node_id: str) -> str:
        if node_id in DATANODE_MAP:
            host, port = DATANODE_MAP[node_id]
            return f"http://{host}:{port}"
        return f"http://{node_id}"
    
    
    def login(self, username: str, password: str) -> bool:
        pass
    
    def upload(self, local_path: str, dfs_path: str) -> bool:
        pass
    
    def download(self, dfs_path: str, local_path: str) -> bool:
        pass
    
    def ls(self, dfs_path: str) -> List[str]:
        pass
    
    def rm(self, dfs_path: str) -> bool:
        pass
    
    def mkdir(self, dfs_path: str) -> bool:
        pass
    
    def rmdir(self, dfs_path: str) -> bool:
        pass