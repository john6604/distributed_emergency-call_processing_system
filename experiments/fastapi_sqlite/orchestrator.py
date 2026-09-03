import json
import sqlite3
import time
from typing import List, Optional

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from pydantic import BaseModel

from emergency_processing.config import env_path, load_env

load_env()

DB_PATH = env_path("TASKS_DB", "experiments/fastapi_sqlite/runtime/tasks.db")
app = FastAPI(title="Orchestrator")

def initialize_database():
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    cursor.executescript(
        """
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
        """
    )
    connection.commit()
    connection.close()


def get_connection():
    return sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)


initialize_database()


class ClaimResponse(BaseModel):
    task_id: int
    conv_id: str
    text: str

class ResultIn(BaseModel):
    task_id: int
    conv_id: str
    keywords: List[str]
    worker: Optional[str] = None


@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    """Insert tasks from a JSONL upload."""
    connection = get_connection()
    cursor = connection.cursor()
    inserted = 0
    try:
        cursor.execute("BEGIN IMMEDIATE")
        for raw in file.file:
            if not raw.strip():
                continue
            record = json.loads(raw.decode("utf-8"))
            conversation_id = str(record.get("id"))
            text = record.get("text", "")
            cursor.execute(
                "INSERT INTO tasks (conv_id, text, status) VALUES (?, ?, 'pending')",
                (conversation_id, text),
            )
            inserted += 1
        connection.commit()
    except Exception as error:
        connection.rollback()
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        connection.close()
    return {"inserted": inserted}


@app.post("/claim", response_model=Optional[ClaimResponse])
def claim(worker: str):
    """Claim one pending task atomically.

    The immediate transaction and conditional update prevent concurrent workers
    from claiming the same row.
    """
    now = int(time.time())
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(
            "SELECT id, conv_id, text FROM tasks "
            "WHERE status='pending' ORDER BY id LIMIT 1"
        )
        row = cursor.fetchone()
        if not row:
            connection.commit()
            return None
        task_id = row[0]
        cursor.execute(
            "UPDATE tasks SET status='processing', worker=?, locked_at=? "
            "WHERE id=? AND status='pending'",
            (worker, now, task_id),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return None
        cursor.execute(
            "SELECT id, conv_id, text FROM tasks WHERE id=?", (task_id,)
        )
        claimed_row = cursor.fetchone()
        connection.commit()
        return ClaimResponse(
            task_id=claimed_row[0], conv_id=claimed_row[1], text=claimed_row[2]
        )
    except Exception as error:
        connection.rollback()
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        connection.close()


@app.post("/result")
def result(data: ResultIn):
    """Store a worker result and mark its task as complete."""
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(
            "UPDATE tasks SET status='done', result=?, worker=?, locked_at=? WHERE id=?",
            (
                json.dumps(data.keywords, ensure_ascii=False),
                data.worker or "unknown",
                int(time.time()),
                data.task_id,
            ),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            raise HTTPException(
                status_code=404, detail="Task was not found or could not be updated."
            )
        connection.commit()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as error:
        connection.rollback()
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        connection.close()


@app.get("/tasks")
def list_tasks(status: Optional[str] = None, limit: int = 100):
    connection = get_connection()
    cursor = connection.cursor()
    try:
        if status:
            cursor.execute(
                "SELECT id, conv_id, status, worker, locked_at, created_at "
                "FROM tasks WHERE status=? ORDER BY id LIMIT ?",
                (status, limit),
            )
        else:
            cursor.execute(
                "SELECT id, conv_id, status, worker, locked_at, created_at "
                "FROM tasks ORDER BY id LIMIT ?",
                (limit,),
            )
        rows = cursor.fetchall()
        return [
            {
                "id": row[0],
                "conv_id": row[1],
                "status": row[2],
                "worker": row[3],
                "locked_at": row[4],
                "created_at": row[5],
            }
            for row in rows
        ]
    finally:
        connection.close()


@app.get("/results")
def get_results(
    limit: int = Query(
        10000,
        ge=1,
        description="Maximum number of results to return; increase for large exports.",
    )
):
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT conv_id, result FROM tasks WHERE status='done' "
            "ORDER BY CAST(conv_id AS INTEGER) LIMIT ?",
            (limit,),
        )
        rows = cursor.fetchall()
        results = []
        for conv_id, result in rows:
            keywords = json.loads(result) if result else []
            results.append({"id": conv_id, "keywords": keywords})
        return results
    finally:
        connection.close()


@app.post("/reclaim-stale")
def reclaim_stale(max_age: int = 120):
    now = int(time.time())
    cutoff = now - max_age
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(
            "UPDATE tasks SET status='pending', worker=NULL, locked_at=NULL "
            "WHERE status='processing' AND locked_at < ?",
            (cutoff,),
        )
        changed = cursor.rowcount
        connection.commit()
        return {"reclaimed": changed}
    except Exception as error:
        connection.rollback()
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        connection.close()


@app.get("/health")
def health():
    return {"ok": True}
