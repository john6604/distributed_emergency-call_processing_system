# orchestrator.py
import sqlite3
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
import json
import time

from emergency_processing.config import env_path, load_env

load_env()

DB_PATH = env_path("TASKS_DB", "experiments/fastapi_sqlite/runtime/tasks.db")
app = FastAPI(title="Orchestrator")

# ---------- DB helpers ----------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conv_id TEXT,
        text TEXT,
        status TEXT DEFAULT 'pending', -- pending | processing | done | failed
        worker TEXT,
        locked_at INTEGER,
        result TEXT,
        created_at INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
    """)
    conn.commit()
    conn.close()

def get_conn():
    # use a fresh connection per request
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    return conn

init_db()

# ---------- Pydantic models ----------
class ClaimResponse(BaseModel):
    task_id: int
    conv_id: str
    text: str

class ResultIn(BaseModel):
    task_id: int
    conv_id: str
    keywords: List[str]
    worker: Optional[str] = None

# ---------- Endpoints ----------
@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    """
    Subir un archivo jsonl con objetos {"id": "...", "text": "..."}
    Inserta filas en tasks.
    """
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    try:
        # start transaction
        cur.execute("BEGIN IMMEDIATE")
        for raw in file.file:
            if not raw.strip():
                continue
            obj = json.loads(raw.decode("utf-8"))
            conv_id = str(obj.get("id"))
            text = obj.get("text","")
            cur.execute(
                "INSERT INTO tasks (conv_id, text, status) VALUES (?, ?, 'pending')",
                (conv_id, text)
            )
            inserted += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
    return {"inserted": inserted}

@app.post("/claim", response_model=Optional[ClaimResponse])
def claim(worker: str):
    """
    Worker atomic claim:
      1) SELECT id FROM tasks WHERE status='pending' ORDER BY id LIMIT 1
      2) UPDATE that row to status='processing', worker=worker, locked_at=now
    Uses a transaction + conditional update to avoid races.
    """
    now = int(time.time())
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT id, conv_id, text FROM tasks WHERE status='pending' ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        task_id = row[0]
        # try to claim
        cur.execute(
            "UPDATE tasks SET status='processing', worker=?, locked_at=? WHERE id=? AND status='pending'",
            (worker, now, task_id)
        )
        if cur.rowcount != 1:
            # someone else claimed it concurrently
            conn.rollback()
            return None
        cur.execute("SELECT id, conv_id, text FROM tasks WHERE id=?", (task_id,))
        r = cur.fetchone()
        conn.commit()
        return ClaimResponse(task_id=r[0], conv_id=r[1], text=r[2])
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/result")
def result(data: ResultIn):
    """
    Worker posts result: saves keywords and marks task done.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            "UPDATE tasks SET status='done', result=?, worker=?, locked_at=? WHERE id=?",
            (json.dumps(data.keywords, ensure_ascii=False), data.worker or 'unknown', int(time.time()), data.task_id)
        )
        if cur.rowcount != 1:
            conn.rollback()
            raise HTTPException(status_code=404, detail="task not found or not updatable")
        conn.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/tasks")
def list_tasks(status: Optional[str] = None, limit: int = 100):
    conn = get_conn()
    cur = conn.cursor()
    try:
        if status:
            cur.execute("SELECT id, conv_id, status, worker, locked_at, created_at FROM tasks WHERE status=? ORDER BY id LIMIT ?", (status, limit))
        else:
            cur.execute("SELECT id, conv_id, status, worker, locked_at, created_at FROM tasks ORDER BY id LIMIT ?", (limit,))
        rows = cur.fetchall()
        return [{"id": r[0], "conv_id": r[1], "status": r[2], "worker": r[3], "locked_at": r[4], "created_at": r[5]} for r in rows]
    finally:
        conn.close()

@app.get("/results")
def get_results(
    limit: int = Query(
        10000,
        ge=1,
        description="Cantidad maxima de resultados a devolver. Aumentar para exportaciones grandes.",
    )
):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT conv_id, result FROM tasks WHERE status='done' ORDER BY CAST(conv_id AS INTEGER) LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
        out = []
        for conv_id, result in rows:
            kws = json.loads(result) if result else []
            out.append({"id": conv_id, "keywords": kws})
        return out
    finally:
        conn.close()

@app.post("/reclaim-stale")
def reclaim_stale(max_age: int = 120):
    now = int(time.time())
    cutoff = now - max_age
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("UPDATE tasks SET status='pending', worker=NULL, locked_at=NULL WHERE status='processing' AND locked_at < ?", (cutoff,))
        changed = cur.rowcount
        conn.commit()
        return {"reclaimed": changed}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/health")
def health():
    return {"ok": True}
