"""DataNode stub — Persona 2 will implement."""

from fastapi import FastAPI

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "stub"}
