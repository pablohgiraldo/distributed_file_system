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
        try:
            response = self._request(
                "POST",
                "/login",
                json={"username": username, "password": password}
            )

            if response.status_code == 200:
                self.username = username
                logger.info(f"Login exitoso como {username}")
                return True
            elif response.status_code == 401:
                raise Exception("Credenciales Invalidas")
            else:
                raise Exception(f"Error de autenticacion: {response.status_code}")
        
        except requests.RequestException as e:
            raise Exception(f"Error conectando NameNode: {e}")

    def upload(self, local_path: str, dfs_path: str) -> bool:
        from tqdm import tqdm
        
        if not os.path.exists(local_path):
            raise Exception(f"El archivo {local_path} no existe")
        
        if not os.path.isfile(local_path):
            raise Exception(f"{local_path} no es un archivo regular")
        
        file_size = os.path.getsize(local_path)
        
        logger.info(f"Iniciando upload de {local_path} ({file_size} bytes) a {dfs_path}")
        
        try:
            response = self._request(
                "POST",
                "/upload/begin",
                json={
                    "filename": dfs_path,
                    "size": file_size,
                    "username": self.username
                }
            )
            response.raise_for_status()
            plan = response.json()
            blocks = plan["blocks"]
            block_size = plan.get("block_size", BLOCK_SIZE)
            
            logger.info(f"Plan recibido: {len(blocks)} bloques, tamaño bloque={block_size}")
            
        except requests.RequestException as e:
            raise Exception(f"Error al iniciar upload: {e}")
        
        success = True
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
                        
                        primary_url = self._get_datanode_url(primary_node)
                        url = f"{primary_url}/block/{block_id}"
                        
                        headers = {
                            "X-Replica-Host": self._get_datanode_host(replica_node),
                            "X-Replica-Port": str(self._get_datanode_port(replica_node))
                        }
                        
                        try:
                            resp = requests.post(url, data=data, headers=headers, timeout=30)
                            resp.raise_for_status()
                            logger.debug(f"Bloque {block_index} (id={block_id}) subido a {primary_node}")
                        except requests.RequestException as e:
                            logger.error(f"Fallo al subir bloque {block_index}: {e}")
                            self._abort_upload(dfs_path)
                            raise Exception(f"Fallo al subir bloque {block_index}: {e}")
                        
                        pbar.update(1)
            
            self._confirm_upload(dfs_path)
            logger.info(f"Upload completado: {file_size} bytes en {len(blocks)} bloques")
            return True
            
        except Exception:
            try:
                self._abort_upload(dfs_path)
            except:
                pass
            raise

    def _get_datanode_host(self, node_id: str) -> str:
        if node_id in DATANODE_MAP:
            return DATANODE_MAP[node_id][0]
        return node_id.split(':')[0] if ':' in node_id else node_id

    def _get_datanode_port(self, node_id: str) -> int:
        if node_id in DATANODE_MAP:
            return DATANODE_MAP[node_id][1]
        return int(node_id.split(':')[1]) if ':' in node_id else 8001

    def _confirm_upload(self, dfs_path: str) -> None:
        response = self._request(
            "POST",
            "/upload/confirm",
            json={"filename": dfs_path, "username": self.username}
        )
        response.raise_for_status()
        logger.info(f"Upload confirmado para {dfs_path}")

    def _abort_upload(self, dfs_path: str) -> None:
        try:
            response = self._request(
                "POST",
                "/upload/abort",
                json={"filename": dfs_path, "username": self.username}
            )
            response.raise_for_status()
            logger.info(f"Upload abortado para {dfs_path}")
        except Exception as e:
            logger.warning(f"Error al abortar upload: {e}")
            
    def download(self, dfs_path: str, local_path: str) -> bool:
        from tqdm import tqdm
    
        try:
            response = self._request("GET", f"/download/{dfs_path.lstrip('/')}")
            response.raise_for_status()
            metadata = response.json()
            blocks = metadata["blocks"]  # Ya vienen ordenados por índice
            expected_size = metadata["size"]
            
            logger.info(f"Descargando {dfs_path}: {expected_size} bytes, {len(blocks)} bloques")
            
        except requests.RequestException as e:
            if response.status_code == 404:
                raise Exception(f"Archivo {dfs_path} no encontrado")
            raise Exception(f"Error al obtener metadata: {e}")
        
        downloaded_bytes = 0
        
        try:
            with open(local_path, 'wb') as f:
                with tqdm(total=len(blocks), desc="Descargando bloques", unit="bloque") as pbar:
                    for block in blocks:
                        block_id = block["id"]
                        block_index = block["index"]
                        primary_node = block["primary"]
                        replica_node = block["replica"]
                        
                        data = None
                        used_replica = False
                        
                        try:
                            primary_url = self._get_datanode_url(primary_node)
                            url = f"{primary_url}/block/{block_id}"
                            resp = requests.get(url, timeout=5)
                            resp.raise_for_status()
                            data = resp.content
                            
                        except (requests.RequestException, TimeoutError) as e:
                            logger.warning(f"Primary {primary_node} falló para bloque {block_index}: {e}")
                            
                            try:
                                replica_url = self._get_datanode_url(replica_node)
                                url = f"{replica_url}/block/{block_id}"
                                resp = requests.get(url, timeout=5)
                                resp.raise_for_status()
                                data = resp.content
                                used_replica = True
                                logger.info(f"Usando réplica {replica_node} para bloque {block_index}")
                                
                            except (requests.RequestException, TimeoutError) as e2:
                                raise Exception(f"Bloque {block_index} no disponible en ningún nodo")

                        f.write(data)
                        downloaded_bytes += len(data)
                        pbar.update(1)
            
            actual_size = os.path.getsize(local_path)
            if actual_size != expected_size:
                logger.warning(f"Tamaño no coincide: esperado {expected_size}, obtenido {actual_size}")
            
            logger.info(f"Download completado: {actual_size} bytes en {local_path}")
            return True
        
        except Exception as e:
            # Limpiar archivo parcial
            if os.path.exists(local_path):
                os.remove(local_path)
            raise
    
    def ls(self, dfs_path: str) -> List[str]:
        pass
    
    def rm(self, dfs_path: str) -> bool:
        pass
    
    def mkdir(self, dfs_path: str) -> bool:
        pass
    
    def rmdir(self, dfs_path: str) -> bool:
        pass