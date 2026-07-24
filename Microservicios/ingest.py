# ingest.py
import os

import requests

try:
    from config import require_env
except ImportError:
    from .config import require_env

ORCH_URL = require_env("ORCH_URL")
JSONL_PATH = os.getenv("MICROSERVICES_JSONL_PATH", "../dataset/conversaciones1.jsonl")

with open(JSONL_PATH, "rb") as f:
    resp = requests.post(f"{ORCH_URL}/ingest", files={"file": ("conversaciones1.jsonl", f, "application/json")}, timeout=300)
    print(resp.status_code, resp.text)
