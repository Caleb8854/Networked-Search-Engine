from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import threading

from .engine import engine

app = FastAPI()

write_lock = threading.Lock()

class SearchReq(BaseModel):
    q: str
    k: int = 10
    k1: float = 1.2
    b: float = 0.75

class SearchHitOut(BaseModel):
    score: float
    doc_id: int
    title: str
    path: str

@app.get("/api/health")
def health():
    return {"ok": True}

@app.post("/api/search", response_model=dict)
def search(req: SearchReq):
    hits = engine.search(req.q, k=req.k, k1=req.k1, b=req.b)
    return {"results": [h.__dict__ for h in hits]}

@app.post("/api/index")
def index(threads: int = 0):
    with write_lock:
        n = engine.index_folder(engine.project_root / "docs", threads=threads)
    return {"indexed": n}

@app.post("/api/merge")
def merge():
    with write_lock:
        live_docs = engine.merge_smallest()
    return {"liveDocs": live_docs}

@app.post("/api/deletepath")
def deletepath(path: str):
    with write_lock:
        affected = engine.delete_path(path)
    return {"affectedSegments": affected}