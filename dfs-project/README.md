# DFS minimalista — NameNode

Sistema de archivos distribuido (bloques): NameNode en Python + FastAPI + gRPC.


## Requisitos

- Docker y Docker Compose

## Arranque rápido (solo NameNode)

1. Copiar metadatos de ejemplo **antes del primer arranque** (usuario `juan` y tres DataNodes ficticios). Si omites este paso, el NameNode crea `metadata.json` sin usuarios y el login fallará.

```bash
cd dfs-project
cp examples/metadata.seed.json data/namenode/metadata.json
```

Si ya arrancaste sin seed y ves `"users": {}` en `data/namenode/metadata.json`, vuelve a ejecutar el `cp` y reinicia: `docker compose restart namenode`.

`HEARTBEAT_TIMEOUT_SEC=999999` en `docker-compose.yml` evita marcar esos nodos como `DEAD` durante pruebas locales sin DataNodes.

2. Levantar el NameNode:

```bash
docker compose up --build namenode
```

Base URL: `http://127.0.0.1:8000`

## Evidencia — comandos `curl`

Sustituye rutas y JSON si tu usuario o archivo difieren.

### Health

```bash
curl -s http://127.0.0.1:8000/health
```

### Login

```bash
curl -s -X POST http://127.0.0.1:8000/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"juan","password":"contrasena123"}'
```

### Upload begin (`BLOCK_SIZE=1024` → varios bloques si `size` es mayor)

```bash
curl -s -X POST http://127.0.0.1:8000/upload/begin \
  -H 'Content-Type: application/json' \
  -d '{"filename":"/juan/video.mp4","size":2500,"username":"juan"}'
```

### Upload confirm

```bash
curl -s -X POST http://127.0.0.1:8000/upload/confirm \
  -H 'Content-Type: application/json' \
  -d '{"filename":"/juan/video.mp4","username":"juan"}'
```

### Download (solo `READY`)

```bash
curl -s http://127.0.0.1:8000/download/juan/video.mp4
```

### Listar directorio

```bash
curl -s http://127.0.0.1:8000/ls/juan
```

### Crear directorio

```bash
curl -s -X POST http://127.0.0.1:8000/mkdir \
  -H 'Content-Type: application/json' \
  -d '{"path":"/juan/docs","username":"juan"}'
```

### Upload abort (entrada `PENDING`)

Primero crea un archivo pendiente sin confirmar:

```bash
curl -s -X POST http://127.0.0.1:8000/upload/begin \
  -H 'Content-Type: application/json' \
  -d '{"filename":"/juan/pending.bin","size":100,"username":"juan"}'
```

```bash
curl -s -X POST http://127.0.0.1:8000/upload/abort \
  -H 'Content-Type: application/json' \
  -d '{"filename":"/juan/pending.bin","username":"juan"}'
```

### Eliminar archivo

```bash
curl -s -X DELETE http://127.0.0.1:8000/rm/juan/video.mp4
```

## gRPC

El NameNode escucha registración y heartbeats en el puerto **50051** (`NameNodeService` definido en `namenode/proto/dfs.proto`).

## Producción / bloques grandes

En `docker-compose.yml`, cambiar por ejemplo:

```yaml
environment:
  - BLOCK_SIZE=67108864
  - HEARTBEAT_TIMEOUT_SEC=30
```

y usar DataNodes reales que envíen heartbeats.

## Estructura

- `namenode/` — FastAPI + gRPC + hilos de fondo (checker + GC de `PENDING`).
- `datanode/` 
- `client/` 
